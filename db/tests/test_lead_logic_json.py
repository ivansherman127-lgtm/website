"""Smoke tests: Python lead flags match bitrix_lead_logic.json (same source as leadLogicSql.ts)."""

from bitrix_lead_quality import _classify_lead_bucket, _load_lead_logic_config


def test_load_config():
    c = _load_lead_logic_config()
    assert "funnels" in c
    assert "Воронка B2B" in c["funnels"]


def test_b2b_alias_and_qual():
    c = _load_lead_logic_config()
    assert _classify_lead_bucket("B2B", "Потенциал 01", "2026-01", c) == "qual"


def test_community_qual_stage():
    c = _load_lead_logic_config()
    assert (
        _classify_lead_bucket("Community", "Заявка квалифицирована (КП Отправлено)", "2026-01", c) == "qual"
    )


def test_career_consult_success():
    c = _load_lead_logic_config()
    assert _classify_lead_bucket("Карьерная консультация", "Сделка успешна", "2026-01", c) == "qual"


def test_demo_access_qual_from_date():
    c = _load_lead_logic_config()
    assert _classify_lead_bucket("Холодная воронка", "Получившие демо-доступ", "2026-01", c) == "unqual"
    assert _classify_lead_bucket("Холодная воронка", "Получившие демо-доступ", "2026-03", c) == "qual"


def test_reactivation_b2b_refusal_cutover():
    c = _load_lead_logic_config()
    # JSON date 2026-03-18 → сравнение по месяцу YYYY-MM (как в leadLogicSql.ts).
    assert _classify_lead_bucket("Реактивация", "B2B отказ", "2026-02", c) == "unqual"
    assert _classify_lead_bucket("Реактивация", "B2B отказ", "2026-03", c) == "qual"
