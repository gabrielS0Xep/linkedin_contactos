import logging
from google.cloud import secretmanager
from google.cloud.secretmanager_v1.types import AccessSecretVersionResponse

class SecretManager:
    """
    SecretManager is a utility class that interacts with Google's Secret Manager Service
    """
    def __init__(self, project:str) -> None:
        self.__logger = logging.getLogger(__name__)
        self.project_id = project
        self.__secret_manager_client = secretmanager.SecretManagerServiceClient()
        
    def get_secret(self, secret_name:str) ->str:
        """Gets a secret from the Google Secret Manager given its name

        Args:
            secret_name (str): The name of the secret to get

        Returns:
            str: The getted secret as a string
        """
        self.__logger.info(f"Getting secret {secret_name} secret manager")

        secret_path = (f"projects/{self.project_id}/secrets/{secret_name}/versions/latest")
        response: AccessSecretVersionResponse = self.__secret_manager_client.access_secret_version(request={"name": secret_path})
        
        return response.payload.data.decode("UTF-8")