from aleph_vrf.models import Executor, ExecutorVRFResponse, PublishedVRFRandomNumber


class VrfException(Exception):
    pass


class AlephNetworkError(VrfException):
    """
    The aleph.im network could not be reached.
    """

    def __str__(self):
        return "Could not reach aleph.im network. Try using a different API server."


class ExecutorHttpError(VrfException):
    """
    An executor does not respond to requests as expected (ex: network errors).
    """

    def __init__(self, url: str, status_code: int, response_text: str):
        self.url = url
        self.status_code = status_code
        self.response_text = response_text

    def __str__(self):
        return f"Error ({self.status_code}) reaching {self.url}: {self.response_text}."


class ExecutorError(Exception):
    """
    An error occurred while communicating with an executor.
    """

    def __init__(self, executor: Executor):
        self.executor = executor

    def __str__(self):
        return f"Executor failed for executor {self.executor.api_url}."


class RandomNumberGenerationFailed(ExecutorError):
    """
    An executor failed to respond when asked to generate a random number.
    """

    def __str__(self):
        return f"Random number generation failed for executor {self.executor.api_url}."


class RandomNumberPublicationFailed(ExecutorError):
    """
    An executor failed to respond when asked to publish a random number.
    """

    def __str__(self):
        return f"Random number publication failed for executor {self.executor.api_url}."


class HashesDoNotMatch(VrfException):
    """
    The random number hash received from /publish is different from the one received from /generate.
    """

    def __init__(self, executor: Executor, generation_hash: str, publication_hash: str):
        self.executor = executor
        self.generation_hash = generation_hash
        self.publication_hash = publication_hash

    def __str__(self):
        return (
            f"Published random number hash ({self.publication_hash})"
            f"does not match the generated one ({self.generation_hash})."
        )


class PublishedHashesDoNotMatch(VrfException):
    """
    The random number hash received from /publish is different from the one received from /generate.
    """

    def __init__(
        self, executor: ExecutorVRFResponse, generation_hash: str, publication_hash: str
    ):
        self.executor = executor
        self.generation_hash = generation_hash
        self.publication_hash = publication_hash

    def __str__(self):
        return (
            f"Published random number hash ({self.publication_hash})"
            f"does not match the generated one ({self.generation_hash})."
        )


class HashValidationFailed(VrfException):
    """
    A random number does not match the SHA3 hash sent by the executor.
    """

    def __init__(
        self,
        random_number: PublishedVRFRandomNumber,
        random_number_hash: str,
        executor: Executor,
    ):
        self.random_number = random_number
        self.random_number_hash = random_number_hash
        self.executor = executor

    def __str__(self):
        return (
            f"The random number published by {self.executor.api_url} "
            f"(execution ID: {self.random_number.execution_id}) "
            "does not match the hash."
        )


class PublishedHashValidationFailed(VrfException):
    """
    A random number does not match the SHA3 hash sent by the executor.
    """

    def __init__(
        self,
        random_number: PublishedVRFRandomNumber,
        random_number_hash: str,
        executor: ExecutorVRFResponse,
    ):
        self.random_number = random_number
        self.random_number_hash = random_number_hash
        self.executor = executor

    def __str__(self):
        return (
            f"The random number published by {self.executor.url} "
            f"(execution ID: {self.random_number.execution_id}) "
            "does not match the hash."
        )


class NotEnoughExecutors(VrfException):
    """
    There are not enough executors available to satisfy the user requirements.
    """

    def __init__(self, requested: int, available: int):
        self.requested = requested
        self.available = available

    def __str__(self):
        return (
            f"Not enough executors available, only {self.available} "
            f"available from {self.requested} requested."
        )
