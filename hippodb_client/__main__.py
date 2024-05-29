import asyncio
import readline  # type: ignore
import shlex
import sys
from typing import cast

import yarl
from yarl import URL
from . import AppID, AuthenticatedHippo, Hippo, HippoToken


async def main():
    hippo = cast(AuthenticatedHippo, None)

    try:
        if len(sys.argv) > 1:
            try:
                url = URL(sys.argv[1])

            except Exception:
                print("Invalid url.", file=sys.stderr)
                return

            hippo = cast(AuthenticatedHippo, await Hippo.create(url))

        while True:
            line = input(">>> ")

            try:
                command = list(shlex.split(line))

            except Exception:
                print("Invalid syntax.")
                continue

            cmd = command[0]

            if cmd == "exit":
                print("Goodbye!", file=sys.stderr)
                break

            elif cmd == "connect":
                try:
                    url = URL(command[1])

                except Exception:
                    print("Invalid url.", file=sys.stderr)
                    continue

                if hippo is not None:
                    await hippo.close()

                hippo = cast(AuthenticatedHippo, await Hippo.create(url))

            elif cmd == "auth":
                auth_hippo = await hippo.authenticate(
                    AppID(command[1]), HippoToken(command[2])
                )

                await hippo.close()

                hippo = auth_hippo

            elif cmd == "list_apps":
                print(await hippo.list_apps())

            elif cmd == "new_app":
                print(await hippo.new_app(" ".join(command[1:])))

            elif cmd == "new_token":
                print(await hippo.new_token(AppID(command[1]), bool(command[2])))

            elif cmd == "delete_app":
                await hippo.delete_app(AppID(command[1]))

            elif cmd == "delete_token":
                pass

            elif cmd == "help":
                print(
                    """==== HippoDB CLI ====
connect url
\tConnect to `url`
auth app-id token
\tAuthenticate as `app-id` using `token`
list_apps
\tList applications
new_app name
\tCreate a new application.
"""
                )

            else:
                print(f"Unknown command {repr(cmd)}", file=sys.stderr)

        if hippo is not None:
            await hippo.close()

    except (KeyboardInterrupt, EOFError):
        if hippo is not None:
            await hippo.close()

        print("Goodbye!", file=sys.stderr)
        return


asyncio.run(main())
