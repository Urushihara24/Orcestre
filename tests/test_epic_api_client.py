import unittest
from unittest import mock


class TestEpicApiClient(unittest.TestCase):
    def test_get_friend_status_maps_summary(self):
        import epic_api_client as c

        client = c.EpicGamesAPIClient(
            login="x",
            password="y",
            proxy_url=None,
            epic_account_id="me",
            device_id="d",
            device_secret="s",
        )

        # Avoid auth path; call _impl directly by patching _with_token_retry.
        def passthrough(fn):
            return fn("TOKEN", "ME")

        with mock.patch.object(client, "_with_token_retry", side_effect=passthrough):
            # friends -> accepted
            with mock.patch.object(
                client,
                "_make_request",
                return_value=(True, {"friends": [{"accountId": "T"}], "outgoing": [], "incoming": []}, None),
            ):
                r = client.get_friend_status("T")
                self.assertTrue(r.ok)
                self.assertEqual(r.code, "accepted")

            # outgoing -> pending
            with mock.patch.object(
                client,
                "_make_request",
                return_value=(True, {"friends": [], "outgoing": [{"accountId": "T"}], "incoming": []}, None),
            ):
                r = client.get_friend_status("T")
                self.assertTrue(r.ok)
                self.assertEqual(r.code, "pending")

            # none -> rejected (not friends)
            with mock.patch.object(
                client,
                "_make_request",
                return_value=(True, {"friends": [], "outgoing": [], "incoming": []}, None),
            ):
                r = client.get_friend_status("T")
                self.assertTrue(r.ok)
                self.assertEqual(r.code, "rejected")

    def test_send_friend_request_is_idempotent_on_conflict(self):
        import epic_api_client as c

        client = c.EpicGamesAPIClient(login="x", password="y", proxy_url=None, epic_account_id="me", device_id="d", device_secret="s")

        def passthrough(fn):
            return fn("TOKEN", "ME")

        with mock.patch.object(client, "_with_token_retry", side_effect=passthrough):
            with mock.patch.object(client, "_make_request", return_value=(False, {"error": "x"}, "conflict")):
                r = client.send_friend_request("T")
                self.assertTrue(r.ok)
                self.assertEqual(r.code, "request_sent")
                self.assertEqual((r.data or {}).get("note"), "idempotent_success")

    def test_send_friend_request_bad_request_is_idempotent_only_with_already_hints(self):
        import epic_api_client as c

        client = c.EpicGamesAPIClient(login="x", password="y", proxy_url=None, epic_account_id="me", device_id="d", device_secret="s")

        def passthrough(fn):
            return fn("TOKEN", "ME")

        with mock.patch.object(client, "_with_token_retry", side_effect=passthrough):
            with mock.patch.object(
                client,
                "_make_request",
                return_value=(False, {"errorMessage": "Request already sent to this user"}, "bad_request"),
            ):
                r = client.send_friend_request("T")
                self.assertTrue(r.ok)
                self.assertEqual(r.code, "request_sent")
                self.assertEqual((r.data or {}).get("note"), "idempotent_success")

            with mock.patch.object(
                client,
                "_make_request",
                return_value=(False, {"errorMessage": "operation blocked by policy"}, "bad_request"),
            ):
                r = client.send_friend_request("T")
                self.assertFalse(r.ok)
                self.assertEqual(r.code, "bad_request")

    def test_ensure_token_no_password_fallback_when_disabled(self):
        import epic_api_client as c

        client = c.EpicGamesAPIClient(
            login="x",
            password="y",
            proxy_url=None,
            epic_account_id="me",
            device_id="d",
            device_secret="s",
            allow_password_fallback=False,
        )

        with mock.patch.object(client, "_is_token_expired", return_value=True):
            with mock.patch.object(client, "_auth_device_auth", return_value=c.ProviderResult(False, "auth_failed", "no")):
                with mock.patch.object(client, "_auth_password", side_effect=AssertionError("password fallback should not be called")):
                    r = client.ensure_token()
                    self.assertFalse(r.ok)

    def test_auth_password_maps_password_grant_blocked(self):
        import epic_api_client as c

        client = c.EpicGamesAPIClient(login="x", password="y", proxy_url=None)
        resp = {
            "errorCode": "errors.com.epicgames.common.oauth.unauthorized_client",
            "errorMessage": "Sorry your client is not allowed to use the grant type password",
        }
        with mock.patch.object(client, "_make_request", return_value=(False, resp, "bad_request")):
            r = client._auth_password()
            self.assertFalse(r.ok)
            self.assertEqual(r.code, "password_grant_blocked")

    def test_set_profile_privacy_level_success(self):
        import epic_api_client as c

        client = c.EpicGamesAPIClient(
            login="x",
            password="y",
            proxy_url=None,
            epic_account_id="me",
            device_id="d",
            device_secret="s",
        )

        def passthrough(fn):
            return fn("TOKEN", "ME")

        calls = []

        def _req(method, url, **kwargs):
            calls.append((method, url, kwargs.get("json")))
            if method == "GET":
                return True, {"acceptInvites": "public", "mutualPrivacy": "ALL"}, "ok"
            return True, {"acceptInvites": "public"}, "ok"

        with mock.patch.object(client, "_with_token_retry", side_effect=passthrough):
            with mock.patch.object(client, "_make_request", side_effect=_req):
                r = client.set_profile_privacy_level("PRIVATE")
                self.assertTrue(r.ok)
                self.assertEqual(r.code, "privacy_settings_updated")
                self.assertEqual(len(calls), 2)
                self.assertEqual(calls[0][0], "GET")
                self.assertEqual(calls[1][0], "PUT")
                self.assertIn("/friends/api/v1/ME/settings", calls[0][1])
                self.assertIn("/friends/api/v1/ME/settings", calls[1][1])
                self.assertEqual((calls[1][2] or {}).get("mutualPrivacy"), "NO_ONE")

    def test_set_profile_privacy_level_rejects_invalid_value(self):
        import epic_api_client as c

        client = c.EpicGamesAPIClient(login="x", password="y", proxy_url=None)
        r = client.set_profile_privacy_level("UNKNOWN")
        self.assertFalse(r.ok)
        self.assertEqual(r.code, "invalid_privacy_level")
