from kaskada.api.session import Builder, Session


class RemoteBuilder(Builder):
    """The RemoteBuilder is a utility class designed to assist in creating remote sessions to the Kaskada Manager service.

    Args:
        Builder (kaskada.api.session.Builder): The Kaskada root builder
    """

    def __init__(self, endpoint: str, is_secure: bool) -> None:
        """Instantiates a RemoteBuilder with the provided endpoint and is secure flag.

        Args:
            endpoint (str): The endpoint of the manager service.
            is_secure (bool): True to use TLS or False to use an insecure connection.
        """
        super().__init__()
        self.endpoint(endpoint, is_secure)

    def build(self) -> Session:
        """Instantiates a Session from the RemoteBuilder instance.

        Returns:
            Session: A Session object
        """
        assert self._is_secure is not None
        assert self._endpoint is not None
        session = Session(self._endpoint, self._is_secure, client_id=self._client_id)
        session.connect(should_check_health=False)
        return session
