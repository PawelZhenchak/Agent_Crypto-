from __future__ import annotations

import asyncio
import os
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from crypto_agent.domain import Decision
from crypto_agent.narrator import (
    NarrativeUnavailable,
    OpenAINarrator,
    _contains_prohibited_recommendation,
)


class NarratorGuardTests(unittest.TestCase):
    def test_direct_recommendation_is_blocked(self) -> None:
        prohibited = (
            "Kup BTC teraz",
            "You should buy BTC",
            "Rozważ zakup BTC",
            "Warto sprzedać ETH",
            "Możesz teraz wejść w BTC",
            "You could increase exposure to ETH",
            "To dobry moment na kupno BTC",
            "Zajmij długą pozycję na ETH",
            "Trzymaj BTC",
            "Zwiększ ekspozycję na ETH",
            "Wyjdź teraz z BTC",
            "Unikaj ETH",
            "Enter BTC now",
            "Exit ETH now",
            "Take a long position in BTC",
            "Hold ETH",
            "Avoid BTC",
            "Increase your exposure to BTC",
            "Rekomenduję zakup BTC.",
            "Polecam zakup BTC.",
            "Zalecam zwiększenie ekspozycji na ETH.",
            "Kupmy BTC teraz.",
            "Sprzedajmy ETH.",
            "Zainwestujmy w BTC.",
            "I recommend purchasing BTC now.",
            "I advise acquiring ETH.",
            "We recommend purchasing BTC.",
        )
        for text in prohibited:
            with self.subTest(text=text):
                self.assertTrue(_contains_prohibited_recommendation(text))

    def test_indirect_recommendation_is_blocked(self) -> None:
        prohibited = (
            "Wejdź teraz w BTC",
            "Otwarcie pozycji na ETH ma sens",
            "Otwarcie pozycji na ETH wydaje się rozsądne",
            "Wejście w BTC jest dobrym pomysłem",
            "Sprzedaż ETH ma sens",
            "Kupno BTC jest zasadne.",
            "Otwarcie pozycji na ETH jest uzasadnione.",
            "Zwiększenie ekspozycji na BTC byłoby korzystne.",
            "Zwiększenie ekspozycji na BTC wygląda atrakcyjnie",
            "To właściwy punkt wejścia w ETH",
            "Opening a position in ETH makes sense",
            "Opening a position in ETH seems sensible",
            "Buying BTC makes sense",
            "Acquiring ETH would be sensible",
            "Purchasing BTC is advisable",
            "Opening a position in ETH would be prudent",
            "Taking a long position in BTC is a good idea",
            "This is a good entry point to buy ETH",
            "Long BTC",
        )
        for text in prohibited:
            with self.subTest(text=text):
                self.assertTrue(_contains_prohibited_recommendation(text))

    def test_unicode_and_whitespace_obfuscation_is_blocked(self) -> None:
        prohibited = (
            "We\u200bjdź teraz w BTC",
            "\ufeffWEJDŹ\tTERAZ\nW BTC",
            "Ｏｐｅｎｉｎｇ a position in ETH makes sense",
            "Otwarcie\tpozycji\nna ETH   ma sens",
            "Otwarcie pozycji na ETH — ma sens",
            "Otwarcie pozycji na ETH, ma sens",
            "Otwarcie pozycji na **ETH** ma sens",
            "Opening a position in ETH — makes sense",
        )
        for text in prohibited:
            with self.subTest(text=text):
                self.assertTrue(_contains_prohibited_recommendation(text))

    def test_neutral_explanation_is_allowed(self) -> None:
        allowed = (
            "System zwrócił NO_SIGNAL z powodu starych danych.",
            "Raport ma status ALERT badawczy; decyzja pochodzi z RiskGate.",
            "Źródła różnią się o 42 pb i mieszczą się w progu polityki.",
            "Dane obejmują 120 zamkniętych świec z Kraken i Coinbase.",
            "System nie otwiera pozycji ani nie wykonuje transakcji.",
            "Raport nie zaleca zwiększenia ekspozycji.",
            "Nie jest to rekomendacja kupna ani sprzedaży.",
            "Open interest is unavailable in this report.",
        )
        for text in allowed:
            with self.subTest(text=text):
                self.assertFalse(_contains_prohibited_recommendation(text))

    def test_non_string_guard_input_is_blocked_fail_closed(self) -> None:
        self.assertTrue(_contains_prohibited_recommendation(None))  # type: ignore[arg-type]

    def test_explain_rejects_bad_summary_even_when_model_marks_it_safe(self) -> None:
        with self.assertRaises(NarrativeUnavailable):
            self._explain_with_fake_sdk(summary="Rekomenduję zakup BTC")

    def test_explain_rejects_bad_uncertainty_even_when_model_marks_it_safe(self) -> None:
        with self.assertRaises(NarrativeUnavailable):
            self._explain_with_fake_sdk(
                summary="System zwrócił NO_SIGNAL z powodu starych danych.",
                uncertainties=["Otwarcie pozycji na ETH ma sens"],
            )

    def test_explain_returns_neutral_output_with_fake_sdk(self) -> None:
        result = self._explain_with_fake_sdk(
            summary="System zwrócił NO_SIGNAL z powodu starych danych.",
            uncertainties=["Źródła różnią się o 42 pb."],
        )
        self.assertEqual(
            result,
            "System zwrócił NO_SIGNAL z powodu starych danych.\n\n"
            "Niepewności:\n- Źródła różnią się o 42 pb.",
        )

    @staticmethod
    def _explain_with_fake_sdk(
        *,
        summary: str,
        uncertainties: list[str] | None = None,
    ) -> str:
        payload = {
            "decision": Decision.NO_SIGNAL.value,
            "summary": summary,
            "uncertainties": uncertainties or [],
            "is_financial_recommendation": False,
        }

        class FakeBaseModel:
            def __init__(self, **values):
                for key, value in values.items():
                    setattr(self, key, value)

        class FakeAgent:
            def __init__(self, **kwargs):
                self.output_type = kwargs["output_type"]

        class FakeRunner:
            @staticmethod
            async def run(agent, _prompt):
                return SimpleNamespace(final_output=agent.output_type(**payload))

        def fake_config_dict(**values):
            return values

        def fake_field(*_args, **_kwargs):
            return None

        agents_module = ModuleType("agents")
        agents_module.Agent = FakeAgent  # type: ignore[attr-defined]
        agents_module.Runner = FakeRunner  # type: ignore[attr-defined]
        pydantic_module = ModuleType("pydantic")
        pydantic_module.BaseModel = FakeBaseModel  # type: ignore[attr-defined]
        pydantic_module.ConfigDict = fake_config_dict  # type: ignore[attr-defined]
        pydantic_module.Field = fake_field  # type: ignore[attr-defined]
        report = SimpleNamespace(
            decision=Decision.NO_SIGNAL,
            to_dict=lambda: {"decision": Decision.NO_SIGNAL.value},
        )
        with (
            patch.dict(
                sys.modules,
                {"agents": agents_module, "pydantic": pydantic_module},
            ),
            patch.dict(
                os.environ,
                {"CRYPTO_AGENT_ENABLE_LLM": "true", "OPENAI_API_KEY": "test-only"},
            ),
        ):
            return asyncio.run(
                OpenAINarrator(model="test-model").explain(report)  # type: ignore[arg-type]
            )


if __name__ == "__main__":
    unittest.main()
