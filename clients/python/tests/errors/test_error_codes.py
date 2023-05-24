import kaskada.errors.error_codes as error_codes


def test_kaskada_doc_link():
    expected_qualified_url = (
        "https://kaskada.io/docs-site/kaskada/main/awkward/tacos/1234#rapid-turles"
    )
    kaskada_doc = error_codes.KaskadaDocLink("/awkward/tacos/1234#rapid-turles")
    assert expected_qualified_url == kaskada_doc.get_qualified_url()


def test_fenl_diagnostic_error():
    error_code = "E1234"
    doc_path = "/awkward/tacos/1234#rapid-turles"
    diagnostic_error = error_codes.FenlDiagnosticError(error_code, doc_path)
    result = diagnostic_error.render_error_code()
    assert (
        result
        == '<a href="https://kaskada.io/docs-site/kaskada/main/awkward/tacos/1234#rapid-turles" target="_blank">E1234</a>'
    )
