from typing import TYPE_CHECKING, NewType, TypeVar, TypedDict
import urllib.parse
import orjson
from pydantic import BaseModel, Json
from aiohttp import BasicAuth, ClientSession
from yarl import URL


ServerInfo = TypedDict(
    "ServerInfo", {"version": str, "features": list[str], "vendor": dict[str, str]}
)
AppID = NewType("AppID", str)
HippoToken = NewType("HippoToken", str)


class ApplicationInfo(BaseModel):
    id: AppID
    name: str


class DatabaseInfo(BaseModel):
    path: str


class Hippo:
    @classmethod
    async def create(cls, url: URL):
        self = cls(_is_from_internal=True)

        self.url = url
        self.session = await ClientSession(
            json_serialize=lambda v: orjson.dumps(v).decode()
        ).__aenter__()

        return self

    def __init__(self, /, _is_from_internal: bool = False):
        if not _is_from_internal:
            raise NotImplemented("Use `await Hippo.create()` instead.")

        self.session: ClientSession
        self.url: URL

    async def close(self):
        await self.session.close()

    async def server_info(self) -> ServerInfo:
        async with self.session.get(self.url.joinpath("api/")) as response:
            return ServerInfo(await response.json())

    async def list_apps(self) -> list[ApplicationInfo]:
        async with self.session.get(self.url.joinpath("api/apps")) as response:
            return [ApplicationInfo(**app) for app in await response.json()]

    async def new_app(self, name: str) -> ApplicationInfo:
        async with self.session.post(
            self.url.joinpath("api/apps/new"), params={"name": name}
        ) as response:
            return ApplicationInfo(**await response.json())

    async def new_token(
        self, application: AppID, writeable: bool = False
    ) -> HippoToken:
        async with self.session.post(
            self.url.joinpath("api/tokens/new"),
            params={"app_id": application, "writeable": str(writeable).lower()},
        ) as response:
            return HippoToken(await response.json())

    async def authenticate(
        self, application: AppID, token: HippoToken
    ) -> "AuthenticatedHippo":
        return await AuthenticatedHippo.create(self.url, application, token)


class AuthenticatedHippo(Hippo):
    @classmethod
    async def create(
        cls, url: URL, application: AppID, token: HippoToken
    ) -> "AuthenticatedHippo":
        self = cls(_is_from_internal=True)

        self.url = url
        self.session = await ClientSession(
            auth=BasicAuth(application, token, encoding="utf-8"),
            json_serialize=lambda v: orjson.dumps(v).decode(),
        ).__aenter__()

        return self

    async def delete_app(self, application: AppID) -> None:
        async with self.session.delete(
            self.url.joinpath("api/apps/delete"), params={"app_id": application}
        ) as response:
            await response.text()

    async def delete_token(self, token: HippoToken) -> None:
        async with self.session.delete(
            self.url.joinpath("api/tokens/delete"), params={"token_id": token}
        ) as response:
            await response.text()

    def _encode_path_segment(self, path_segment: str) -> str:
        return urllib.parse.quote(urllib.parse.quote(path_segment, safe=""), safe="")

    async def create_database(self, path: str) -> DatabaseInfo:
        async with self.session.post(
            self.url.joinpath("api/create_db"), params={"path": path}
        ) as response:
            return DatabaseInfo(**await response.json())

    async def list_dbs(
        self, path: str = "/", recursive: bool = False
    ) -> list[DatabaseInfo]:
        async with self.session.get(
            self.url.joinpath(
                "api/dbs/" + self._encode_path_segment(path), encoded=True
            ),
            params={"recursive": str(recursive).lower()},
        ) as response:
            return [DatabaseInfo(**v) for v in await response.json()]

    async def list_documents(self, path: str) -> list[str]:
        async with self.session.get(
            self.url.joinpath("api/" + self._encode_path_segment(path), encoded=True)
        ) as response:
            return await response.json()

    async def delete_database(self, path: str) -> None:
        async with self.session.delete(
            self.url.joinpath("api/" + self._encode_path_segment(path), encoded=True)
        ) as response:
            await response.text()

    async def create_document(
        self, database_path: str, name: str, contents: list[Json] | dict[str, Json]
    ) -> str:
        async with self.session.post(
            self.url.joinpath(
                "api/" + self._encode_path_segment(database_path), encoded=True
            ),
            params={"document_name": name},
            json=contents,
        ) as response:
            return await response.json()

    T = TypeVar("T", bound=list[Json] | dict[str, Json])

    async def read_document(
        self, database_path: str, name: str
    ) -> list[Json] | dict[str, Json]:
        async with self.session.get(
            self.url.joinpath(
                "api/"
                + self._encode_path_segment(database_path)
                + "/"
                + self._encode_path_segment(name),
                encoded=True,
            ),
        ) as response:
            return await response.json()

    async def document_exists(self, database_path: str, name: str) -> bool:
        async with self.session.get(
            self.url.joinpath(
                "api/"
                + self._encode_path_segment(database_path)
                + "/"
                + self._encode_path_segment(name)
                + "/exists",
                encoded=True,
            ),
        ) as response:
            return await response.json()

    async def update_document(
        self, database_path: str, name: str, contents: dict[str, Json] | list[Json]
    ) -> None:
        async with self.session.put(
            self.url.joinpath(
                "api/"
                + self._encode_path_segment(database_path)
                + "/"
                + self._encode_path_segment(name),
                encoded=True,
            ),
            json=contents,
        ) as response:
            await response.text()

    async def delete_document(self, database_path: str, name: str) -> None:
        async with self.session.delete(
            self.url.joinpath(
                "api/"
                + self._encode_path_segment(database_path)
                + "/"
                + self._encode_path_segment(name),
                encoded=True,
            )
        ) as response:
            await response.text()
