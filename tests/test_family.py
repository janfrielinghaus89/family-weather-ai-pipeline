import pytest
from pydantic import ValidationError

from family import (
    Child,
    FamilyProfile,
    FamilyAdvice,
    recommendation_key,
)


def test_child_age():
    child = Child(age_months=7)
    assert child.age_months == 7


def test_invalid_child_age():
    with pytest.raises(ValidationError):
        Child(age_months=-1)


def test_family_requires_children():
    with pytest.raises(ValidationError):
        FamilyProfile(children=[])


def test_family_supports_multiple_children():
    profile = FamilyProfile(
        children=[Child(age_months=7), Child(age_months=36)], interests=["Natur"]
    )

    assert len(profile.children) == 2


def test_recommendation_key_is_stable():
    profile = FamilyProfile(children=[Child(age_months=12)], interests=["Tiere"])

    first = recommendation_key("weather-1", profile)
    second = recommendation_key("weather-1", profile)

    assert first == second


def test_recommendation_key_changes_with_profile():
    first = FamilyProfile(children=[Child(age_months=6)])
    second = FamilyProfile(children=[Child(age_months=24)])

    assert recommendation_key("weather-1", first) != recommendation_key(
        "weather-1", second
    )


def test_advice_requires_three_activities():
    with pytest.raises(ValidationError):
        FamilyAdvice(summary="Mildes Wetter", clothing=[], activities=["Spaziergang"])
