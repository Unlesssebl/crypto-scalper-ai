"""Telegram-friendly rendering of bot statistics."""

from __future__ import annotations

import html


def _esc(value) -> str:
    return html.escape(str(value), quote=False)


def _b(value: str, rich: bool) -> str:
    return f"<b>{value}</b>" if rich else value


def _bps(value) -> str:
    if value is None:
        return "—"
    return f"{value:+.1f}"


def _interval(interval) -> str:
    if not interval:
        return "мало блоков"
    low, high = interval[0], interval[1]
    return f"[{low:+.1f}; {high:+.1f}]"


def _percent(value) -> str:
    return "—" if value is None else f"{100 * value:.0f}%"


def _pct_interval(interval) -> str:
    if not interval:
        return ""
    return f" [{100 * interval[0]:.0f}%; {100 * interval[1]:.0f}%]"


def bot_card(report: dict, *, rich: bool = True) -> str:
    """Full card for one bot, suitable for /bot <id>."""
    if report.get("implemented") is False:
        return (
            f"⚙️ {_b(_esc(report['id']), rich)} v{report.get('version', '?')} — "
            "контракт объявлен, триггер ещё не реализован.\n"
            f"Гипотеза: {_esc(report.get('hypothesis', ''))}"
        )
    if not report.get("observations"):
        return (
            f"🤖 {_b(_esc(report['id']), rich)} v{report.get('version', '?')} · "
            f"{report.get('window', '?')}\n"
            f"Гипотеза: {_esc(report.get('hypothesis', ''))}\n"
            "Наблюдений пока нет. Бот ждёт первых эпизодов."
        )

    primary = report.get("primary") or {}
    horizon = report.get("horizons", {}).get(str(report.get("primary_horizon_s", 300)), {})
    mode = report.get("mode")
    horizon_label = f"{report.get('primary_horizon_s', 300) // 60} мин"
    lines = [
        f"🤖 {_b(_esc(report['id']), rich)} v{report.get('version', '?')} · "
        f"{_esc(report.get('window', '?'))} · {_esc(report.get('status', ''))}",
        f"Гипотеза: {_esc(report.get('hypothesis', ''))}",
        f"Наблюдений: {report.get('observations', 0)} · оценено: {report.get('scored', 0)} "
        f"({_percent((report.get('coverage') or 0))})",
    ]

    if mode == "directional":
        lines.append(
            f"{horizon_label}: {_bps(horizon.get('mean_bps'))} б.п. {_interval(horizon.get('ci'))} · "
            f"hit {_percent(horizon.get('hit_rate'))}{_pct_interval(horizon.get('hit_ci'))}"
        )
    else:
        lines.append(
            f"{horizon_label}: |ход| {_bps(horizon.get('mean_bps'))} б.п. {_interval(horizon.get('ci'))}"
        )

    ranks = horizon.get("ranks") or []
    strong = [row for row in ranks if row["strength"] >= 3]
    weak = [row for row in ranks if row["strength"] in (1, 2)]
    if strong:
        strong_n = sum(row["scored"] for row in strong)
        strong_mean = (
            sum((row["mean_bps"] or 0) * row["scored"] for row in strong) / strong_n if strong_n else None
        )
        lines.append(f"Сила 3–4: N={strong_n} · {_bps(strong_mean)} б.п.")
    if weak:
        weak_n = sum(row["scored"] for row in weak)
        weak_mean = (
            sum((row["mean_bps"] or 0) * row["scored"] for row in weak) / weak_n if weak_n else None
        )
        lines.append(f"Сила 1–2: N={weak_n} · {_bps(weak_mean)} б.п.")

    if primary.get("kind") == "rank_difference":
        if primary.get("diff_bps") is not None:
            q = f" · q={primary['q']:.3f}" if primary.get("q") is not None else ""
            lines.append(
                f"Разница 3–4 минус 1–2: {_bps(primary['diff_bps'])} б.п. "
                f"{_interval(primary.get('ci'))} · p={primary.get('p') if primary.get('p') is not None else '—'}{q}"
            )
        else:
            lines.append("Разница силы: недостаточно наблюдений в одной из групп.")
    elif primary.get("kind") == "excess_absolute":
        q = f" · q={primary['q']:.3f}" if primary.get("q") is not None else ""
        lines.append(
            f"Превышение контроля: {_bps(primary.get('mean_bps'))} б.п. "
            f"{_interval(primary.get('ci'))} · p={primary.get('p') if primary.get('p') is not None else '—'}{q}"
        )

    excess = horizon.get("excess") or {}
    lines.append(
        f"Контроль: {_bps(excess.get('mean_bps'))} б.п. {_interval(excess.get('ci'))} "
        f"(N={horizon.get('control_scored', 0)})"
    )
    lines.append(
        f"MAE/MFE: {_bps(horizon.get('mae_median_bps'))} / {_bps(horizon.get('mfe_median_bps'))} б.п."
    )
    lines.append(
        f"Версия: {report.get('version')} · contract {_esc(report.get('contract_hash', ''))}"
    )
    if report.get("note"):
        lines.append(f"Примечание: {_esc(report['note'])}")
    return "\n".join(lines)


def bots_summary(reports: list[dict], *, rich: bool = True) -> str:
    """Compact list for /bots."""
    lines = ["🤖 Наблюдатели · live", ""]
    for report in reports:
        if report.get("implemented") is False:
            lines.append(f"⚙️ {_esc(report['id'])} — ожидает реализации")
            continue
        if not report.get("observations"):
            lines.append(f"• {_esc(report['id'])} — нет наблюдений")
            continue
        primary = report.get("primary") or {}
        value = primary.get("diff_bps") if primary.get("kind") == "rank_difference" else primary.get("mean_bps")
        q = f" · q={primary['q']:.2f}" if primary.get("q") is not None else ""
        lines.append(
            f"• {_b(_esc(report['id']), rich)} — {report.get('scored', 0)}/{report.get('observations', 0)} "
            f"({_percent(report.get('coverage') or 0)}), 5 мин {_bps(value)} б.п. "
            f"{_interval(primary.get('ci'))}{q} · {_esc(report.get('status', ''))}"
        )
    if len(lines) == 2:
        lines.append("Нет объявленных наблюдателей.")
    lines.append("")
    lines.append("Подробно: /bot flow.continuation")
    return "\n".join(lines)
