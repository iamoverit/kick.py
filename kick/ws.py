from __future__ import annotations

import json
from os import execve
from typing import TYPE_CHECKING, Type

from aiohttp import ClientWebSocketResponse as WebSocketResponse
import logging
from .livestream import PartialLivestream
from .message import Message

if TYPE_CHECKING:
    from .http import HTTPClient

LOG = logging.getLogger(__name__)

__all__ = ()


class PusherWebSocket:
    def __init__(self, ws: WebSocketResponse, *, http: HTTPClient):
        self.ws = ws
        self.http = http
        self.send_json = ws.send_json
        self.close = ws.close
        self.socket_id = None

    async def poll_event(self) -> None:
        raw_msg = await self.ws.receive()
        LOG.debug(f"WS received: {raw_msg}")
        try:
            raw_data = raw_msg.json()
        except TypeError:
            raw_data = raw_msg

        self.http.client.dispatch("raw_payload_receive", raw_data)
        if isinstance(raw_data, dict):
            event = raw_data.get('event')
            if event is not None:
                try:
                    data = json.loads(raw_data.get("data"))
                except (TypeError, KeyError, json.decoder.JSONDecodeError):
                    data = {}
                self.http.client.dispatch("payload_receive", event, data)

                match event:
                    case "pusher:connection_established":
                        self.socket_id = data["socket_id"]
                    case "App\\Events\\ChatMessageEvent":
                        msg = Message(data=data, http=self.http)
                        self.http.client.dispatch("message", msg)
                    case "App\\Events\\StreamerIsLive":
                        livestream = PartialLivestream(data=data.get("livestream"), http=self.http)
                        self.http.client.dispatch("livestream_start", livestream)
                    case "App\\Events\\StopStreamBroadcast":
                        self.http.client.dispatch("livestream_stop", data.get("livestream"))
                    case "App\\Events\\FollowersUpdated":
                        user = self.http.client._watched_users[data["channel_id"]]
                        if data.get("followed"):
                            event = "follow"
                            user._data["followers_count"] += 1
                        else:
                            event = "unfollow"
                            user._data["followers_count"] -= 1

                        self.http.client.dispatch(event, user)
                    case _:
                        self.http.client.dispatch("other", event, data, raw_data.get("channel"))

    async def start(self) -> None:
        while not self.ws.closed:
            await self.poll_event()

    async def subscribe_to_chatroom(self, chatroom_id: int) -> None:
        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"chatroom_{chatroom_id}"},
            }
        )
        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
            }
        )
        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}"},
            }
        )

    async def unsubscribe_to_chatroom(self, chatroom_id: int) -> None:
        await self.send_json(
            {
                "event": "pusher:unsubscribe",
                "data": {"auth": "", "channel": f"chatrooms.{chatroom_id}.v2"},
            }
        )

    async def watch_channel(self, channel_id: int) -> None:
        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"channel.{channel_id}"},
            }
        )
        
        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"channel_{channel_id}"},
            }
        )

        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"predictions-channel-{channel_id}"},
            }
        )

    async def watch_channel_private(self, livestream_id: int) -> None:
        private_livestream_channel = f"private-livestream.{livestream_id}"
        private_livestream_auth = await self.http.broadcasting_auth(private_livestream_channel, self.socket_id)

        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": private_livestream_auth["auth"], "channel": private_livestream_channel},
            }
        )

        private_userfeed_channel = f"private-userfeed.{self.http.client.user.id}"
        private_userfeed_auth = await self.http.broadcasting_auth(private_userfeed_channel, self.socket_id)

        private_channelpoints_channel = f"private-channelpoints-{self.http.client.user.id}"
        private_channelpoints_auth = await self.http.broadcasting_auth(private_channelpoints_channel, self.socket_id)

        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": private_userfeed_auth["auth"], "channel": private_userfeed_channel},
            }
        )

        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": private_channelpoints_auth["auth"], "channel": private_channelpoints_channel},
            }
        )


    async def unwatch_channel(self, channel_id: int) -> None:
        await self.send_json(
            {
                "event": "pusher:subscribe",
                "data": {"auth": "", "channel": f"channel.{channel_id}"},
            }
        )
