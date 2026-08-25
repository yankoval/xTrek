from importlib import resources


def test_report_templates_and_styles_are_package_resources():
    root = resources.files("xtrek.reports")

    assert root.joinpath("templates", "html", "report.html.jinja").is_file()
    assert root.joinpath("templates", "markdown", "report.md.jinja").is_file()
    assert root.joinpath("styles", "profiles", "browser.css").is_file()
    assert root.joinpath("styles", "profiles", "printer.css").is_file()
    assert root.joinpath("styles", "profiles", "messenger.css").is_file()
