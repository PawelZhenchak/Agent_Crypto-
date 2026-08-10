from __future__ import annotations

import unittest

from crypto_agent.narrator import _contains_prohibited_recommendation


class NarratorGuardTests(unittest.TestCase):
    def test_direct_recommendation_is_blocked(self) -> None:
        self.assertTrue(_contains_prohibited_recommendation("Kup BTC teraz"))
        self.assertTrue(_contains_prohibited_recommendation("You should buy BTC"))
        self.assertTrue(_contains_prohibited_recommendation("Rozważ zakup BTC"))
        self.assertTrue(_contains_prohibited_recommendation("Warto sprzedać ETH"))
        self.assertTrue(
            _contains_prohibited_recommendation("To dobry moment na kupno BTC")
        )

    def test_neutral_explanation_is_allowed(self) -> None:
        self.assertFalse(
            _contains_prohibited_recommendation(
                "System zwrócił NO_SIGNAL z powodu starych danych."
            )
        )


if __name__ == "__main__":
    unittest.main()
