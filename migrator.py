import base64
from dataclasses import dataclass
import datetime
import json
import logging
import requests
import time
from typing import Any, Callable, Dict, Iterable, Optional


@dataclass
class SetFieldOperation:
    ado_field: str
    yt_field: str
    set_after_creation: bool


CustomFieldHandler = Callable[[Dict[str, Any]], Iterable[SetFieldOperation]]


class Migrator:
    def __init__(
        self,
        token_azo: str,
        yt_base: str,
        ado_organization: str,
        ado_project: str,
        token_youtrack: Optional[str] = None,
    ):
        """
        Initialize the Migrator class with the necessary parameters for YouTrack and Azure DevOps.

        :param token_azo: Personal Access Token for Azure DevOps
        :param yt_base: Base URL for YouTrack instance
        :param ado_organization: Azure DevOps organization URL
        :param ado_project: Azure DevOps project name
        :param token_youtrack: Optional token for YouTrack authentication
        """
        self.yt_base = yt_base
        self.ado_base = f"{ado_organization}/{ado_project}/_apis/wit"
        self.auth_header_ado = self._authorization_header_ado(token_azo)
        self.auth_header_youtrack = (
            token_youtrack and self._authorization_header_youtrack(token_youtrack)
        )

    @staticmethod
    def _authorization_header_ado(pat: str) -> str:
        """
        Generate the authorization header for Azure DevOps using the provided Personal Access Token (PAT).

        :param pat: Personal Access Token for Azure DevOps
        :return: Authorization header string
        """
        return "Basic " + base64.b64encode(f":{pat}".encode("ascii")).decode("ascii")

    @staticmethod
    def _authorization_header_youtrack(token: str) -> str:
        """
        Generate the authorization header for YouTrack using the provided token.

        :param token: Token for YouTrack authentication
        :return: Authorization header string
        """
        return "Bearer " + token

    @staticmethod
    def _set_field(ado_field: str, yt_field: str) -> Dict[str, Optional[str]]:
        """
        Create a dictionary representing a field operation for Azure DevOps work item.

        :param ado_field: Azure DevOps field name
        :param yt_field: YouTrack field value
        :return: Dictionary representing the field operation
        """
        return {
            "op": "add",
            "path": f"/fields/{ado_field}",
            "from": None,
            "value": yt_field,
        }

    @staticmethod
    def _format_yt_timestamp(timestamp: int) -> str:
        """
        Convert a YouTrack timestamp to ISO 8601 format.

        :param timestamp: YouTrack timestamp in milliseconds
        :return: ISO 8601 formatted timestamp
        """
        return datetime.datetime.utcfromtimestamp(timestamp // 1000).isoformat()

    def _youtrack_issue_data(self, yt_id: str):
        """
        Retrieve issue data from YouTrack for the given issue ID.

        :param yt_id: YouTrack issue ID
        :return: Dictionary containing issue data
        """
        # We take the list of custom field keys from
        # https://www.jetbrains.com/help/youtrack/standalone/api-howto-get-issues-with-all-values.html
        yt_fields = (
            "customFields(name,value(avatarUrl,buildLink,color(id),fullName,id,"
            "isResolved,localizedName,login,minutes,name,presentation,text)),"
            "created,reporter(login),summary,description,"
            "comments(created,author(login),text),attachments(base64Content,name)"
        )
        yt_url = f"{self.yt_base}/api/issues/{yt_id}?fields={yt_fields}"
        headers = {}
        if self.auth_header_youtrack:
            headers["Authorization"] = self.auth_header_youtrack
        yt_data = requests.get(yt_url, verify=False, headers=headers).json()
        return yt_data

    @staticmethod
    def _build_custom_field_dict(yt_data: Dict) -> Dict:
        """
        Build a dictionary of custom fields from YouTrack issue data.

        :param yt_data: Dictionary containing YouTrack issue data
        :return: Dictionary of custom fields
        """
        return {v["name"]: v["value"] for v in yt_data["customFields"]}

    def custom_fields(self, yt_id: str) -> Dict:
        """
        Retrieve custom fields for a given YouTrack issue ID.

        :param yt_id: YouTrack issue ID
        :return: Dictionary of custom fields
        """
        yt_data = self._youtrack_issue_data(yt_id)
        return self._build_custom_field_dict(yt_data)

    def migrate_issue(
        self, yt_id: str, custom_field_handler: CustomFieldHandler,
    ):
        """
        Migrate a single YouTrack issue to Azure DevOps.

        :param yt_id: YouTrack issue ID
        :param custom_field_handler: Function to handle custom fields during migration
        """
        create_ops = []  # Operations to perform on new Azure DevOps work item
        yt_data = self._youtrack_issue_data(yt_id)

        # Handle general information about issue
        summary = yt_data["summary"]
        create_ops.append(self._set_field("System.Title", summary))

        created = self._format_yt_timestamp(yt_data["created"])
        description = (
            f'[Migrated from <a href="{self.yt_base}/issue/{yt_id}">YouTrack</a>, '
            f'originally reported by {yt_data["reporter"]["login"]} on {created}]'
            f'\n\n{yt_data["description"]}'
        )
        description = description.replace("\n", "<br />\n")
        create_ops.append(self._set_field("System.Description", description))

        # Handle custom fields
        fields = self._build_custom_field_dict(yt_data)
        delayed_ops = []
        for custom_op in custom_field_handler(fields):
            op = self._set_field(custom_op.ado_field, custom_op.yt_field)
            (delayed_ops if custom_op.set_after_creation else create_ops).append(op)

        # Create new work item in Azure DevOps boards and get its ID
        # https://docs.microsoft.com/en-us/rest/api/azure/devops/wit/work%20items/create?view=azure-devops-rest-6.0
        res = requests.post(
            f"{self.ado_base}/workitems/$Task?api-version=6.0",
            headers={
                "Authorization": self.auth_header_ado,
                "Content-Type": "application/json-patch+json",
            },
            json=create_ops,
        ).json()
        if "id" not in res:
            raise RuntimeError(f"migration of {yt_id} failed: {res}")
        work_item_id = res["id"]

        # Perform all operations that can only be performed after the work item has
        # been created
        requests.patch(
            f"{self.ado_base}/workitems/{work_item_id}?api-version=6.0",
            headers={
                "Authorization": self.auth_header_ado,
                "Content-Type": "application/json-patch+json",
            },
            json=delayed_ops,
        )

        # Move all comments from YouTrack issue to the work item created above
        # https://docs.microsoft.com/en-us/rest/api/azure/devops/wit/comments/add?view=azure-devops-rest-6.0
        for comment in yt_data["comments"]:
            created = self._format_yt_timestamp(comment["created"])
            author = comment["author"]["login"]
            text = (
                f'[Migrated from <a href="{self.yt_base}/issue/{yt_id}">YouTrack</a>. '
                f"Original comment by {author} on {created}]"
                f'\n\n{comment["text"]}'
            )
            text = text.replace("\n", "<br/>\n")
            requests.post(
                f"{self.ado_base}/workItems/{work_item_id}"
                "/comments?api-version=6.0-preview.3",
                headers={
                    "Authorization": self.auth_header_ado,
                    "Content-Type": "application/json",
                },
                json={"text": text},
            )

        # Move all attachments as well, keeping track of the file name used on YouTrack
        # https://docs.microsoft.com/en-us/rest/api/azure/devops/wit/attachments/create?view=azure-devops-rest-6.0
        # https://docs.microsoft.com/en-us/rest/api/azure/devops/wit/work%20items/update?view=azure-devops-rest-6.0#add-an-attachment
        for attachment in yt_data["attachments"]:
            name = attachment["name"]
            # We need to do this in two steps: First upload an attachment ...
            b64content = attachment["base64Content"].split(",")[1]
            decoded = base64.b64decode(b64content)
            res = requests.post(
                f"{self.ado_base}/attachments?api-version=6.0",
                headers={
                    "Authorization": self.auth_header_ado,
                    "Content-Type": "application/octet-stream",
                },
                data=decoded,
            )

            # ... then take the URL of the newly created attachment and add that to
            # the work item
            attachment_url = res.json()["url"]
            attachment_data = [
                {
                    "op": "add",
                    "path": "/relations/-",
                    "value": {
                        "rel": "AttachedFile",
                        "url": attachment_url,
                        "attributes": {"name": name},
                    },
                }
            ]
            requests.patch(
                f"{self.ado_base}/workItems/{work_item_id}?api-version=6.0",
                headers={
                    "Authorization": self.auth_header_ado,
                    "Content-Type": "application/json-patch+json",
                },
                json=attachment_data,
            )

    def migrate_project(
        self,
        yt_project: str,
        custom_field_handler: CustomFieldHandler,
        issue_count_upper_limit: int = 10000,
    ):
        """
        Migrate all issues from a YouTrack project to Azure DevOps.

        :param yt_project: YouTrack project ID
        :param custom_field_handler: Function to handle custom fields during migration
        :param issue_count_upper_limit: Upper limit for the number of issues to migrate
        """
        headers = {}
        if self.auth_header_youtrack:
            headers["Authorization"] = self.auth_header_youtrack
        issues = requests.get(
            f"{self.yt_base}/api/issues?fields=idReadable"
            f"&$top={issue_count_upper_limit}"
            f"&query=project:+{yt_project}",
            verify=False,
            headers=headers,
        ).json()
        for i, issue in enumerate(issues):
            yt_id = issue["idReadable"]
            logging.info(f"Migrating {yt_id}, {i + 1}/{len(issues)}")
            # When handling large migrations, the Azure DevOps will occasionally return
            # empty responses. We handle this by retrying after a while.
            while True:
                try:
                    self.migrate_issue(yt_id, custom_field_handler)
                    break
                except json.decoder.JSONDecodeError:
                    logging.info("Encountered an error. Wait 3 seconds")
                    time.sleep(3)
            logging.info(f"Migrated {yt_id}, {i + 1}/{len(issues)}")
