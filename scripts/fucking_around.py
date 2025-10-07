import dropbox

APP_KEY = "ilx6grkgm5zt4dd"
APP_SECRET = "d937pkqtxg9kgul"

auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(APP_KEY, APP_SECRET, token_access_type="offline")
authorize_url = auth_flow.start()
print("1. Go to:", authorize_url)
print("2. Click Allow, copy the authorization code.")
auth_code = input("Enter the authorization code here: ").strip()

oauth_result = auth_flow.finish(auth_code)
print("Refresh token:", oauth_result.refresh_token)
