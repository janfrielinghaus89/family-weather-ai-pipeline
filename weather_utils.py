"""Pure presentation helpers for the WeatherWatcher dashboard."""


def format_number(value: int | float | None, suffix: str = "") -> str:
    """Format a numeric weather value for German-language output."""
    if value is None:
        return "–"
    try:
        number = f"{float(value):.1f}".replace(".", ",")
    except (TypeError, ValueError, OverflowError):
        return "–"
    return f"{number}{suffix}"


def get_sentiment_style(score: int) -> tuple[str, str, str]:
    """Return text, background and label for an AI sentiment score."""
    if score > 7:
        return "#166534", "#dcfce7", "Sehr gutes Wetter"
    if score >= 5:
        return "#854d0e", "#fef9c3", "Durchschnittliches Wetter"
    return "#991b1b", "#fee2e2", "Ungünstiges Wetter"
