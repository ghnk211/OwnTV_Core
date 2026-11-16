package tv.own.owntv.core.update

import android.app.PendingIntent
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.ApplicationInfo
import android.content.pm.PackageInfo
import android.content.pm.PackageInstaller
import android.content.pm.PackageManager
import android.os.Build
import android.util.Log
import androidx.core.content.ContextCompat
import androidx.core.content.IntentCompat
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import org.json.JSONObject
import tv.own.owntv.core.CoreBuildInfo
import java.io.File
import java.io.IOException
import java.security.MessageDigest

/**
 * In-app updates straight from GitHub Releases: checks the repo's latest release, compares its tag
 * with the installed version, downloads the release APK, and hands it to the system installer.
 * No server of our own — each app's releases CI already publishes one APK per ABI flavour per tag
 * (`OwnTV-vX.Y.Z.apk` / `OwnTV-x86_64-vX.Y.Z.apk` on the television, `OwnTV-Mobile-…` on the phone),
 * and the asset matching this device's ABI is chosen, so updates also work on an x86_64 emulator.
 * **Which repository is asked is the host app's to say** — see [CoreBuildInfo.releaseRepo].
 */
class UpdateManager(
    private val context: Context,
    private val client: OkHttpClient,
    private val settings: tv.own.owntv.core.settings.SettingsRepository,
) {
    data class UpdateInfo(
        val version: String,
        val notes: String,
        val apkUrl: String,
        /** GitHub's `assets[].digest` (e.g. `sha256:...`); null on releases that predate it. */
        val expectedDigest: String? = null,
    )

    sealed interface Failure {
        data class CheckHttp(val code: Int) : Failure
        data object NoCompatibleApk : Failure
        data object InvalidReleaseResponse : Failure
        data object CheckNetwork : Failure
        data class DownloadHttp(val code: Int) : Failure
        data object EmptyDownload : Failure
        data object DownloadNetwork : Failure
        data object Install : Failure

        /** Internal storage cannot hold the APK twice (download + install session). */
        data class NotEnoughSpace(val requiredBytes: Long) : Failure

        /** The file that arrived is truncated or not a readable OwnTV package — never install it. */
        data object DamagedDownload : Failure

        /** The system installer refused the package; [message] is its own wording when it gave one. */
        data class InstallRejected(val message: String?) : Failure
    }

    sealed interface State {
        data object Idle : State
        data object Checking : State
        data object UpToDate : State
        data class Available(val info: UpdateInfo) : State
        data class Downloading(val percent: Int) : State
        data class Failed(val failure: Failure, val retryInfo: UpdateInfo? = null) : State
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    /** Downloads follow redirects by hand, so each hop can be checked (see [executeUpdateRequest]). */
    private val updateClient: OkHttpClient by lazy {
        client.newBuilder()
            .followRedirects(false)
            .followSslRedirects(false)
            .build()
    }

    private val _state = MutableStateFlow<State>(State.Idle)
    val state: StateFlow<State> = _state.asStateFlow()

    private class CheckHttpException(val code: Int) : IOException()
    private class DownloadHttpException(val code: Int) : IOException()
    private class NoCompatibleApkException : IOException()
    private class InvalidReleaseResponseException : IOException()
    private class EmptyDownloadException : IOException()
    private class NotEnoughSpaceException(val requiredBytes: Long) : IOException()
    private class DamagedDownloadException(reason: String) : IOException(reason)

    private fun failureFor(error: Throwable, checking: Boolean): Failure = when (error) {
        is CheckHttpException -> Failure.CheckHttp(error.code)
        is DownloadHttpException -> Failure.DownloadHttp(error.code)
        is NoCompatibleApkException -> Failure.NoCompatibleApk
        is InvalidReleaseResponseException -> Failure.InvalidReleaseResponse
        is EmptyDownloadException -> Failure.EmptyDownload
        is NotEnoughSpaceException -> Failure.NotEnoughSpace(error.requiredBytes)
        is DamagedDownloadException -> Failure.DamagedDownload
        else -> if (checking) Failure.CheckNetwork else Failure.DownloadNetwork
    }

    val currentVersion: String = CoreBuildInfo.versionName

    /** Queries GitHub's latest release; moves to Available / UpToDate / a semantic failure. */
    fun check() {
        if (_state.value is State.Checking || _state.value is State.Downloading) return
        _state.value = State.Checking
        scope.launch {
            runCatching {
                val request = Request.Builder()
                    .url("https://api.github.com/repos/${CoreBuildInfo.releaseRepo}/releases/latest")
                    .header("Accept", "application/vnd.github+json")
                    .header("User-Agent", "OwnTV")
                    .build()
                client.newCall(request).execute().use { resp ->
                    if (!resp.isSuccessful) throw CheckHttpException(resp.code)
                    val body = resp.body.string()
                    if (body.isBlank()) throw InvalidReleaseResponseException()
                    val o = runCatching { JSONObject(body) }.getOrElse { throw InvalidReleaseResponseException() }
                    val version = o.optString("tag_name").removePrefix("v").takeIf { it.isNotBlank() }
                        ?: throw InvalidReleaseResponseException()
                    val notes = o.optString("body").take(16_000)
                    val assets = o.optJSONArray("assets") ?: throw InvalidReleaseResponseException()
                    // Releases carry one APK per ABI flavor (arm = generic, x86_64 suffixed). Never
                    // silently install an APK for the wrong ABI.
                    val wantX86 = Build.SUPPORTED_ABIS.firstOrNull() == "x86_64"
                    val asset = (0 until assets.length())
                        .asSequence()
                        .mapNotNull { assets.optJSONObject(it) }
                        .firstOrNull { candidate ->
                            val name = candidate.optString("name")
                            val url = candidate.optString("browser_download_url")
                            name.endsWith(".apk", ignoreCase = true) &&
                                url.isNotBlank() &&
                                name.contains("x86_64", ignoreCase = true) == wantX86
                        }
                        ?: throw NoCompatibleApkException()
                    val apkUrl = asset.optString("browser_download_url")
                    // Same-origin with the download URL: it detects truncation/corruption, it does
                    // not prove publisher identity. The signer check below is unconditional.
                    val digest = asset.optString("digest").takeIf { it.isNotBlank() }
                    val info = UpdateInfo(version, notes, apkUrl, digest)
                    settings.recordUpdateCheck(System.currentTimeMillis())
                    if (isNewer(version, currentVersion)) _state.value = State.Available(info)
                    else _state.value = State.UpToDate
                }
            }.onFailure { error ->
                Log.w(TAG, "update check failed: ${error.message}", error)
                _state.value = State.Failed(failureFor(error, checking = true))
            }
        }
    }

    /** Downloads the release APK with progress, then opens the system installer. */
    fun downloadAndInstall() {
        val info = (_state.value as? State.Available)?.info ?: return
        _state.value = State.Downloading(0)
        scope.launch {
            runCatching {
                val dir = File(context.filesDir, "updates").apply { mkdirs() }
                val out = File(dir, "owntv-update.apk")
                out.delete() // never build on top of a previous half-download
                val request = Request.Builder().url(info.apkUrl).header("User-Agent", "OwnTV").build()
                try {
                    executeUpdateRequest(request).use { resp ->
                        if (!resp.isSuccessful) throw DownloadHttpException(resp.code)
                        val body = resp.body
                        val total = body.contentLength()
                        // The APK is written once here and copied again into the install session, so
                        // the download needs room for two of it. Checking up front turns a silent short
                        // write — which reaches the installer as "App not installed" — into a real message.
                        if (total > 0 && dir.usableSpace < total * 2) throw NotEnoughSpaceException(total * 2)
                        val digest = MessageDigest.getInstance("SHA-256")
                        var copied = 0L
                        body.byteStream().use { input ->
                            out.outputStream().use { output ->
                                val buf = ByteArray(64 * 1024)
                                while (true) {
                                    val n = input.read(buf)
                                    if (n < 0) break
                                    output.write(buf, 0, n)
                                    digest.update(buf, 0, n)
                                    copied += n
                                    if (total > 0) _state.value = State.Downloading((copied * 100 / total).toInt())
                                }
                            }
                        }
                        if (copied == 0L) throw EmptyDownloadException()
                        if (total > 0 && copied != total) {
                            throw DamagedDownloadException("truncated: got $copied of $total bytes")
                        }
                        // Computed while writing, so the bytes verified are exactly the bytes kept.
                        verifyDigest(digest, info.expectedDigest)
                    }
                    verifyApk(out)
                    install(out)
                } catch (e: Throwable) {
                    out.delete() // a bad file must not linger and be retried as-is
                    throw e
                }
                _state.value = State.Available(info) // dialog stays sane if the user cancels install
            }.onFailure { error ->
                Log.w(TAG, "update download failed: ${error.message}", error)
                val failure = if (error is InstallException) Failure.Install else failureFor(error, checking = false)
                _state.value = State.Failed(failure, retryInfo = info)
            }
        }
    }

    /**
     * 校验下载包是否为可安装的官方更新。
     *
     * 校验顺序：可解析性 -> 包名 -> debuggable -> 签名 -> 版本递增。
     * 任意一项失败都抛出 [DamagedDownloadException]，由调用方删除文件并提示用户。
     *
     * @param apk 已落盘的更新 APK
     */
    @Suppress("DEPRECATION")
    private fun verifyApk(apk: File) {
        val signatureFlags = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            PackageManager.GET_SIGNING_CERTIFICATES
        } else {
            PackageManager.GET_SIGNATURES
        }
        val archive = context.packageManager.getPackageArchiveInfo(apk.absolutePath, signatureFlags)
            ?: throw DamagedDownloadException("unparseable APK (${apk.length()} bytes)")
        if (archive.packageName != context.packageName) {
            throw DamagedDownloadException("wrong package ${archive.packageName}")
        }
        // getPackageArchiveInfo 在部分实现上不填 applicationInfo；null 不能等同于 debuggable，
        // 否则会把所有合法更新误判为调试包。
        val flags = archive.applicationInfo?.flags
            ?: throw DamagedDownloadException("missing applicationInfo")
        if ((flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0) {
            throw DamagedDownloadException("debuggable update")
        }
        val current = context.packageManager.getPackageInfo(context.packageName, signatureFlags)
        if (!sameSigners(archive, current)) {
            throw DamagedDownloadException("signer mismatch")
        }
        // PackageInfo.longVersionCode 是 API 28 字段，minSdk 26；API 26/27 直接访问会抛
        // NoSuchFieldError，必须走 versionCode 分支。
        val newVersion = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            archive.longVersionCode
        } else {
            archive.versionCode.toLong()
        }
        val oldVersion = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            current.longVersionCode
        } else {
            current.versionCode.toLong()
        }
        if (newVersion <= oldVersion) {
            throw DamagedDownloadException("version not newer")
        }
    }

    /**
     * 比较两个包的签名集合是否完全一致。
     *
     * 使用集合比较而不是"任一匹配"，避免多签名包只匹配其中一个证书就被放行。
     */
    private fun sameSigners(archive: PackageInfo, current: PackageInfo): Boolean {
        val left = signerHashes(archive)
        val right = signerHashes(current)
        return left.isNotEmpty() && left == right
    }

    /**
     * 取一个包的签名证书 SHA-256 集合。
     *
     * API 28+ 走 signingInfo.apkContentsSigners，API 26-27 走已废弃的 signatures。
     * 若未来启用 v3 轮换，需要按 signingCertificateHistory/lineage 判断。
     */
    @Suppress("DEPRECATION")
    private fun signerHashes(info: PackageInfo): Set<String> {
        val signatures = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.P) {
            info.signingInfo?.apkContentsSigners.orEmpty()
        } else {
            info.signatures.orEmpty()
        }
        return signatures.map { signature ->
            MessageDigest.getInstance("SHA-256")
                .digest(signature.toByteArray())
                .joinToString("") { byte -> "%02x".format(byte) }
        }.toSet()
    }

    /**
     * 校验 GitHub Release asset 的 sha256 摘要。
     *
     * 老版本 Release 没有 digest 字段时跳过，由签名校验兜底，避免把老版本用户挡在更新之外。
     *
     * @param digest 下载过程中同步计算的摘要
     * @param expectedDigest GitHub 返回的 digest，可为 null
     */
    private fun verifyDigest(digest: MessageDigest, expectedDigest: String?) {
        if (expectedDigest.isNullOrBlank()) return
        val algorithm = expectedDigest.substringBefore(':').uppercase()
        val expected = expectedDigest.substringAfter(':', "").lowercase()
        if (algorithm != "SHA256" || expected.isBlank()) {
            throw DamagedDownloadException("unsupported digest $expectedDigest")
        }
        val actual = digest.digest().joinToString("") { byte -> "%02x".format(byte) }
        if (!actual.equals(expected, ignoreCase = true)) {
            throw DamagedDownloadException("digest mismatch")
        }
    }

    /**
     * 跟随 GitHub 的下载重定向，并逐跳校验目标 host。
     *
     * Releases 会 302 到 objects.githubusercontent.com 或 release-assets.githubusercontent.com；
     * 只检查初始 github.com 等于没检查。任何非白名单地址都拒绝。
     */
    private fun executeUpdateRequest(request: Request): okhttp3.Response {
        var current = request
        repeat(MAX_UPDATE_REDIRECTS + 1) {
            requireAllowedUpdateUrl(current.url)
            val response = updateClient.newCall(current).execute()
            if (response.code !in 300..399) return response
            val location = response.header("Location")
            response.close()
            if (location.isNullOrBlank()) {
                throw DamagedDownloadException("redirect without Location")
            }
            current = current.newBuilder()
                .url(current.url.resolve(location) ?: throw DamagedDownloadException("bad redirect $location"))
                .build()
        }
        throw DamagedDownloadException("too many redirects")
    }

    /** 更新链只允许 GitHub 的 HTTPS 主机；见 network_security_config.xml 的同名域名。 */
    private fun requireAllowedUpdateUrl(url: okhttp3.HttpUrl) {
        if (url.scheme != "https" || url.host !in UPDATE_HOSTS) {
            throw DamagedDownloadException("blocked update url ${url.host}")
        }
    }

    /**
     * Installs through [PackageInstaller] rather than an `ACTION_VIEW` intent on a FileProvider URI.
     * Two reasons: the session reads the APK from our own storage (no cross-app URI grant to go wrong
     * on older TV installers), and its status broadcast carries the real reason a package was refused
     * — insufficient storage, a signature conflict, a bad file — instead of the system's silent
     * "App not installed".
     */
    private fun install(apk: File) {
        // Second read of the same path: narrows the verifyApk() -> install() TOCTOU window.
        verifyApk(apk)
        registerInstallReceiver()
        val installer = context.packageManager.packageInstaller
        val params = PackageInstaller.SessionParams(PackageInstaller.SessionParams.MODE_FULL_INSTALL)
        val sessionId = try {
            installer.createSession(params)
        } catch (e: IOException) {
            throw InstallException(e)
        }
        installer.openSession(sessionId).use { session ->
            session.openWrite(SESSION_ENTRY, 0, apk.length()).use { output ->
                apk.inputStream().use { it.copyTo(output) }
                session.fsync(output)
            }
            val intent = Intent(INSTALL_STATUS_ACTION).setPackage(context.packageName)
            val flags = PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_MUTABLE
            session.commit(PendingIntent.getBroadcast(context, 0, intent, flags).intentSender)
        }
        apk.delete() // the session owns its own copy now
    }

    /** Registered once, for the app's lifetime: the confirmation round trip outlives any one call. */
    private fun registerInstallReceiver() {
        if (installReceiverRegistered) return
        installReceiverRegistered = true
        ContextCompat.registerReceiver(
            context,
            installStatusReceiver,
            IntentFilter(INSTALL_STATUS_ACTION),
            ContextCompat.RECEIVER_NOT_EXPORTED,
        )
    }

    private var installReceiverRegistered = false

    private val installStatusReceiver = object : BroadcastReceiver() {
        override fun onReceive(ctx: Context, intent: Intent) {
            val status = intent.getIntExtra(PackageInstaller.EXTRA_STATUS, PackageInstaller.STATUS_FAILURE)
            val message = intent.getStringExtra(PackageInstaller.EXTRA_STATUS_MESSAGE)
            when (status) {
                // The user still has to approve the install — this is the system prompt, not a failure.
                PackageInstaller.STATUS_PENDING_USER_ACTION -> {
                    val confirm = IntentCompat.getParcelableExtra(intent, Intent.EXTRA_INTENT, Intent::class.java)
                    confirm?.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
                    runCatching { confirm?.let { context.startActivity(it) } }
                        .onFailure { _state.value = State.Failed(Failure.Install) }
                }
                PackageInstaller.STATUS_SUCCESS -> Log.i(TAG, "update installed")
                // Declining the system prompt is a choice, not an error — leave the dialog as it was.
                PackageInstaller.STATUS_FAILURE_ABORTED -> Log.i(TAG, "install cancelled by user")
                else -> {
                    Log.w(TAG, "install refused status=$status message=$message")
                    _state.value = State.Failed(Failure.InstallRejected(message?.takeIf { it.isNotBlank() }))
                }
            }
        }
    }

    /** Retries the failed phase without losing a successfully resolved release asset. */
    fun retry() {
        val failed = _state.value as? State.Failed ?: return
        val info = failed.retryInfo
        if (info != null && failed.failure !is Failure.CheckHttp && failed.failure !is Failure.NoCompatibleApk &&
            failed.failure !is Failure.InvalidReleaseResponse && failed.failure !is Failure.CheckNetwork
        ) {
            _state.value = State.Available(info)
            downloadAndInstall()
        } else {
            check()
        }
    }

    fun reset() {
        if (_state.value !is State.Downloading) _state.value = State.Idle
    }

    /** Numeric segment-wise compare: "1.10.0" > "1.9.3"; non-numeric junk compares as 0. */
    private fun isNewer(remote: String, local: String): Boolean {
        val r = remote.split('.').map { it.takeWhile(Char::isDigit).toIntOrNull() ?: 0 }
        val l = local.split('.').map { it.takeWhile(Char::isDigit).toIntOrNull() ?: 0 }
        for (i in 0 until maxOf(r.size, l.size)) {
            val a = r.getOrElse(i) { 0 }
            val b = l.getOrElse(i) { 0 }
            if (a != b) return a > b
        }
        return false
    }

    private class InstallException(cause: Throwable) : IOException(cause)

    companion object {
        private const val TAG = "UpdateManager"
        private const val SESSION_ENTRY = "owntv-update"
        private const val INSTALL_STATUS_ACTION = "tv.own.owntv.UPDATE_INSTALL_STATUS"
        private const val MAX_UPDATE_REDIRECTS = 5
        private val UPDATE_HOSTS = setOf(
            "github.com",
            "api.github.com",
            "objects.githubusercontent.com",
            "release-assets.githubusercontent.com",
        )
    }
}
