import base64
from dataclasses import dataclass
import datetime
import json
import logging
import os
import requests
import time
from typing import Any, Callable, Dict, Iterable, Optional
from urllib.parse import quote

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

@dataclass
class SetFieldOperation:
    ado_field: str
    yt_field: Any
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
        self.yt_base = yt_base
        self.ado_base = f"{ado_organization}/{ado_project}/_apis/wit"
        self.auth_header_ado = self._authorization_header_ado(token_azo)
        self.auth_header_youtrack = (
            token_youtrack and self._authorization_header_youtrack(token_youtrack)
        )

    @staticmethod
    def _authorization_header_ado(pat: str) -> str:
        return "Basic " + base64.b64encode(f":{pat}".encode("ascii")).decode("ascii")

    @staticmethod
    def _authorization_header_youtrack(token: str) -> str:
        return "Bearer " + token

    @staticmethod
    def _set_field(ado_field: str, yt_field: Any) -> Dict[str, Optional[Any]]:
        return {
            "op": "add",
            "path": f"/fields/{ado_field}",
            "from": None,
            "value": yt_field,
        }

    @staticmethod
    def _format_yt_timestamp(timestamp: int) -> str:
        return datetime.datetime.utcfromtimestamp(timestamp // 1000).isoformat()

    def _youtrack_issue_data(self, yt_id: str):
        yt_fields = (
            "customFields(name,value(avatarUrl,buildLink,color(id),fullName,id,"
            "isResolved,localizedName,login,minutes,name,presentation,text)),"
            "created,reporter(login),summary,description,"
            "comments(created,author(login),text,attachments(url,name,id)),"
            "attachments(url,name,id)"
        )
        yt_url = f"{self.yt_base}/api/issues/{yt_id}?fields={yt_fields}"
        headers = {}
        if self.auth_header_youtrack:
            headers["Authorization"] = self.auth_header_youtrack
        yt_data = requests.get(yt_url, verify=False, headers=headers).json()
        return yt_data

    @staticmethod
    def _build_custom_field_dict(yt_data: Dict) -> Dict:
        return {v["name"]: v["value"] for v in yt_data["customFields"]}

    def custom_fields(self, yt_id: str) -> Dict:
        yt_data = self._youtrack_issue_data(yt_id)
        return self._build_custom_field_dict(yt_data)

    def _download_attachment(self, url: str, filename: str) -> bytes:
        response = requests.get(url, verify=False)
        logging.info(f"Downloading from URL: {url}")
        logging.info(f"Response status code: {response.status_code}")
        logging.info(f"Response content type: {response.headers.get('Content-Type')}")
        logging.info(f"Response content length: {response.headers.get('Content-Length')}")
        response.raise_for_status()
        content = response.content
        logging.info(f"First 100 bytes of content: {content[:100]}")
        unique_filename = f"downloaded_{filename}"
        with open(unique_filename, 'wb') as f:
            f.write(content)
        logging.info(f"Downloaded attachment saved to {unique_filename} with size {os.path.getsize(unique_filename)} bytes")
        return content

    def _upload_attachment(self, name: str, content: bytes) -> str:
        res = requests.post(
            f"{self.ado_base}/attachments?fileName={name}&api-version=7.1",
            headers={
                "Authorization": self.auth_header_ado,
                "Content-Type": "application/octet-stream",
            },
            data=content,
        )
        return res.json()["url"]

    def migrate_issue(
        self, yt_id: str, custom_field_handler: CustomFieldHandler,
    ):
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
        for comment in yt_data["comments"]:
            created = self._format_yt_timestamp(comment["created"])
            author = comment["author"]["login"]
            text = (
                f'[Migrated from <a href="{self.yt_base}/issue/{yt_id}">YouTrack</a>. '
                f"Original comment by {author} on {created}]"
                f'\n\n{comment["text"]}'
            )
            text = text.replace("\n", "<br/>\n")

            # Handle attachments in comments
            for attachment in comment.get("attachments", []):
                logging.info(f"Downloading attachment {attachment['name']} from comment in {yt_id}")
                attachment_url = attachment.get("url")
                if not attachment_url:
                    logging.error(f"Attachment {attachment['name']} does not have a valid URL.")
                    continue
                if not attachment_url.startswith("http"):
                    attachment_url = f"{self.yt_base}{attachment_url}"
                logging.info(f"Attachment URL: {attachment_url}")
                attachment_content = self._download_attachment(attachment_url, attachment["name"])
                logging.info(f"Uploading attachment {attachment['name']} to Azure DevOps")
                uploaded_attachment_url = self._upload_attachment(attachment["name"], attachment_content)
                text += f'<br/><a href="{uploaded_attachment_url}">{attachment["name"]}</a>'

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
        for attachment in yt_data["attachments"]:
            logging.info(f"Downloading attachment {attachment['name']} from issue {yt_id}")
            attachment_url = attachment.get("url")
            if not attachment_url:
                logging.error(f"Attachment {attachment['name']} does not have a valid URL.")
                continue
            if not attachment_url.startswith("http"):
                attachment_url = f"{self.yt_base}{attachment_url}"
            logging.info(f"Attachment URL: {attachment_url}")
            attachment_content = self._download_attachment(attachment_url, attachment["name"])
            logging.info(f"Uploading attachment {attachment['name']} to Azure DevOps")
            uploaded_attachment_url = self._upload_attachment(attachment["name"], attachment_content)
            attachment_data = [
                {
                    "op": "add",
                    "path": "/relations/-",
                    "value": {
                        "rel": "AttachedFile",
                        "url": uploaded_attachment_url,
                        "attributes": {"name": attachment["name"]},
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
        headers = {}
        if self.auth_header_youtrack:
            headers["Authorization"] = self.auth_header_youtrack
        yt_project_encoded = quote(yt_project)
        response = requests.get(
            f"{self.yt_base}/api/issues?fields=idReadable"
            f"&$top={issue_count_upper_limit}"
            f"&query=project:{yt_project_encoded}",
            verify=False,
            headers=headers,
        )
        print(response.text)  # Debugging line to print the response content
        if response.status_code != 200:
            raise RuntimeError(f"Failed to fetch issues: {response.text}")
        issues = response.json()
        for i, issue in enumerate(issues):
            yt_id = issue["idReadable"]
            logging.info(f"Migrating {yt_id}, {i + 1}/{len(issues)}")
            while True:
                try:
                    self.migrate_issue(yt_id, custom_field_handler)
                    break
                except json.decoder.JSONDecodeError:
                    logging.info("Encountered an error. Wait 3 seconds")
                    time.sleep(3)
            logging.info(f"Migrated {yt_id}, {i + 1}/{len(issues)}")


# Define your variables
token_azo = "1OdqxDpm5OpVNxsATuGT6gRoAE5Vi0qVfkmgOCnN1Eoig6efHJb1JQQJ99ALACAAAAAAAAAAAAASAZDOk2UH"
yt_base = "https://ryano0oceros.youtrack.cloud"
ado_organization = "https://dev.azure.com/ryfal24"
ado_project = "boardtarget"
token_youtrack = "perm:YWRtaW4=.NDYtMQ==.24d6Jq7uVl5BDdPOhPw6cdNW3b7z0u"  # Ensure this is set correctly

# Create an instance of the Migrator class
migrator = Migrator(
    token_azo=token_azo,
    yt_base=yt_base,
    ado_organization=ado_organization,
    ado_project=ado_project,
    token_youtrack=token_youtrack
)

# Define a custom field handler
def custom_field_handler(fields):
    priority = fields.get("Priority", {}).get("name", "")
    # Map YouTrack priority values to Azure DevOps integer values
    priority_mapping = {
        "Show-stopper": 1,
        "Critical": 2,
        "Major": 3,
        "Normal": 4,
        "Minor": 5
    }
    priority_value = priority_mapping.get(priority, 4)  # Default to Normal if not found
    yield SetFieldOperation("Microsoft.VSTS.Common.Priority", priority_value, False)

# Migrate all issues from a YouTrack project
yt_project = "migrate"
migrator.migrate_project(yt_project, custom_field_handler, issue_count_upper_limit=50000)