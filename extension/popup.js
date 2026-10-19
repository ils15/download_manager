const HOST_NAME = "org.independent.download_manager";
const AUTH_KEYS = new Set([
  "token", "accesstoken", "access_token", "refreshtoken", "refresh_token", "idtoken", "id_token",
  "auth", "auth_token", "authorization", "client_secret", "secret", "api_key", "apikey", "key",
  "signature", "sig", "password", "passwd", "credential", "credentials", "session", "sessionid",
  "session_id", "sessiontoken", "session_token", "cookie", "awsaccesskeyid", "googleaccessid"
]);
const button = document.getElementById("send");
const status = document.getElementById("status");

button.addEventListener("click", async () => {
  button.disabled = true;
  status.textContent = "Checking the selected tab…";
  try {
    const [tab] = await chrome.tabs.query({ active: true, lastFocusedWindow: true });
    if (!tab?.url || !/^https?:\/\//i.test(tab.url)) {
      throw new Error("Only an HTTP(S) tab can be sent.");
    }
    const selected = new URL(tab.url);
    const hasAuthParameter = [...selected.searchParams.keys()].some((key) => {
      const normalized = key.toLowerCase();
      return AUTH_KEYS.has(normalized)
        || (normalized.startsWith("x-amz-") && /credential|signature|security-token/.test(normalized))
        || (normalized.startsWith("x-goog-") && /credential|signature/.test(normalized));
    });
    if (selected.username || selected.password || hasAuthParameter) {
      throw new Error("URLs containing embedded credentials or auth tokens are not accepted.");
    }
    selected.hash = "";
    const reply = await chrome.runtime.sendNativeMessage(HOST_NAME, { action: "download", url: selected.href });
    if (!reply?.accepted) throw new Error("The local host did not accept this URL.");
    status.textContent = "URL sent. The local app will add it to the queue.";
  } catch (error) {
    status.textContent = error instanceof Error ? error.message : "Could not contact the local download manager.";
  } finally {
    button.disabled = false;
  }
});
