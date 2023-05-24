from typing import Dict

from domonic.html import a

KASKADA_DOC_ENDPOINT = "https://kaskada.io/docs-site/kaskada/main"


class KaskadaDocLink:
    """Encapsulates a Kaskada Document Link"""

    def __init__(self, path: str):
        """Instantiates a Kaskada Document Link

        Args:
            path (str): the path to append to KASKADA_DOC_ENDPOINT
        """
        self.path = path

    def get_qualified_url(self) -> str:
        """Gets the fully qualified URL for the document link.

        Returns:
            str: the fully qualified URL (endpoint + path)
        """
        return f"{KASKADA_DOC_ENDPOINT}{self.path}"


class FenlDiagnosticError:
    """Represents a Fenl Diagnostic Error"""

    def __init__(self, code: str, doc_path: str):
        """Instantiates a Fenl Diagnostic Error

        Args:
            code (str): The error code from the Engine e.g. E0010
            doc_path (str): The doc path for more information
        """
        self.code = code
        self.link = KaskadaDocLink(doc_path)

    def render_error_code(self) -> str:
        """Renders the error code as a string as <a href="<fully qualifed url>" target="_blank">code</a>

        Returns:
            str: the string a html element
        """
        return str(a(self.code, _href=self.link.get_qualified_url(), _target="_blank"))


E0013 = FenlDiagnosticError("E0013", "/fenl/fenl-diagnostic-codes.html#E0013")

FENL_DIAGNOSTIC_ERRORS: Dict[str, FenlDiagnosticError] = {"E0013": E0013}
