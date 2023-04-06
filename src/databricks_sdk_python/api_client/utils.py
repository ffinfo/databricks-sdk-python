from requests import Response


class UnknownApiResponse(Exception):
    def __init__(self, response: Response):
        self.response = response
        self.message = (
            f"Response: {self.response.json()}, Status code: {response.status_code}, Url: {response.request.url}"
        )
        super().__init__(self.message)
