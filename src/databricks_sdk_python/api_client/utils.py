from requests import Response


class UnknownApiResponse(Exception):
    def __init__(self, response: Response):
        self.response = response
        self.message = self.response.json()
        super().__init__(self.message)
