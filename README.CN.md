[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-success.svg)](https://mit-license.org/)
[![pypi 0.2.0](https://img.shields.io/badge/pypi-0.2.0-ff69b4.svg)](https://pypi.org/project/fastapi-distributed-websocket/)

# FastAPI 分布式 WebSocket

基于 FastAPI 的分布式系统 WebSocket 实现库。

**注意：该库仍处于早期阶段，在生产环境中使用需自行承担风险。**

## 功能概述

该库的主要功能包括：

* 轻松实现广播、发布/订阅（pub/sub）、聊天室等功能
* 代理 WebSocket 连接到其他服务器（例如从 API 网关转发连接）
* 认证支持
* 清晰的异常处理机制
* 内存消息代理（broker），便于快速开发

## WebSocket 在多服务器环境下的扩展性问题

WebSocket 是一种相对较新的协议，用于基于 HTTP 的实时通信。  
它可以在客户端与服务器之间建立持久的、有状态的全双工连接，适用于实现聊天、实时通知、广播以及发布/订阅模型。

### 客户端连接问题

HTTP 的请求/响应机制在生产环境下易于扩展到多个服务器实例。  
客户端每次发起请求时，可以连接到任意服务器实例，并收到相同的响应。  
响应返回后，客户端断开连接，并可以在下次请求时连接到不同的服务器实例，  
这得益于 HTTP 的无状态（stateless）特性。

然而，WebSocket 连接是有状态（stateful）的，一旦连接建立，  
如果发生错误导致连接丢失，就需要确保客户端能够重新连接到**原来的服务器实例**，  
因为该实例存储着该连接的状态。

**有状态（stateful）意味着存在可被操作的状态，尤其是 WebSocket 连接，  
它依赖于自身的状态才能正常运行。**

### 广播和群组消息问题

在 WebSocket 扩展性方面，另一个常见问题是如何向多个已连接的客户端发送消息（如广播或群组消息）。

假设我们有一个聊天服务器，当用户在某个聊天室中发送消息时，  
需要将该消息广播给所有订阅该聊天室的用户。  
在单服务器实例下，所有连接都由该实例管理，因此消息能够可靠地传递给所有接收者。  
但在多服务器实例的情况下，订阅同一聊天室的用户可能会连接到不同的服务器实例。  
这样一来，如果用户在服务器 *A* 的聊天室 *"xyz"* 发送了一条消息，  
但聊天室 *"xyz"* 中部分用户连接的是服务器 *B*，他们将不会收到该消息。

### WebSocket 接口的文档化问题

WebSocket 的另一个问题（与扩展性无关）是文档化的困难。  
由于 WebSocket 是基于事件驱动的协议，因此难以用 [OpenAPI](https://swagger.io/specification/) 进行描述。  
不过，近期出现了一种新的规范专门用于描述异步、事件驱动的接口，  
它被称为 [AsyncAPI](https://www.asyncapi.com/)。  
目前我正在研究该规范，不确定是否需要在本库中实现，  
还是另行开发一个独立的库，但无论如何，这是一个值得关注的问题。

### 其他问题

在构思该库时，我查阅了 StackOverflow、Reddit、GitHub Issues 等平台，  
调研了 WebSocket 相关的常见问题。  
我整理了一些有价值的资源，并结合最佳方案，设计了本库的解决方案。

## 示例

### 安装

```sh
pip install fastapi-distributed-websocket
```

### 基本用法

以下是一个基本示例，使用 **单个服务器实例** ，并基于 **内存消息代理（in-memory broker）** 进行管理。

```python
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status
from distributed_websocket import Connection, WebSocketManager

app = FastAPI()
manager = WebSocketManager('channel:1', broker_url='memory://')
...


@app.on_event('startup')
async def startup() -> None:
    ...
    await manager.startup()


@app.on_event('shutdown')
async def shutdown() -> None:
    ...
    await manager.shutdown()


@app.websocket('/ws/{conn_id}')
async def websocket_endpoint(
    ws: WebSocket,
    conn_id: str,
    *,
    topic: Optional[Any] = None,
) -> None:
    connection: Connection = await manager.new_connection(ws, conn_id)
    try:
        while True:
            msg = await connection.receive_json()
            await manager.broadcast(msg)
    except WebSocketDisconnect:
        await manager.remove_connection(connection)
```

`manager.new_connection` 方法会创建一个新的 `Connection` 对象，并将其添加到 `manager.active_connections` 列表中。  
注意：当 `WebSocketDisconnect` 异常被触发时，我们会调用 `remove_connection`，  
但该方法**不会**调用 `connection.close`，因为 WebSocket 连接已经关闭了。  
如果你需要在其他情况下手动关闭连接，可以使用 `manager.close_connection` 方法。

如果使用 `connection.iter_json`，它会自动处理 `WebSocketDisconnect` 异常，因此可以直接在循环结束后调用 `manager.remove_connection`（参见下方代码示例）。

此外，上述示例中，我们使用 `manager.broadcast` 方法将消息发送给所有由 `WebSocketManager` 实例管理的连接。  
**但该方法仅适用于单服务器实例的情况。**  
在多服务器实例的环境下，应该使用 `manager.receive` 方法，将消息正确地发送到消息代理（broker）。

```python
@app.websocket('/ws/{conn_id}')
async def websocket_endpoint(
    ws: WebSocket,
    conn_id: str,
    *,
    topic: Optional[Any] = None,
) -> None:
    connection: Connection = await manager.new_connection(ws, conn_id)
    # 推荐的 WebSocketDisconnect 处理方式
    async for msg in connection.iter_json():
        await manager.receive(connection, msg)
    await manager.remove_connection(connection)
```

### API 网关代理 WebSocket

假设我们正在开发一个**聊天服务**，且所有后端服务都在**API 网关**后面。  
如果 WebSocket 也需要保持在 API 网关之后，`fastapi-distributed-websocket` 提供了 `WebSocketProxy` 来进行代理。

```python
from distributed_websocket import WebSocketProxy
# 省略其他导入

app = FastAPI()

WS_TARGET_ENDPOINT = 'ws://websocket_service:8000/wshandler'

@app.websocket('/ws')
async def websocket_proxy(websocket: WebSocket):
    await websocket.accept()
    ws_proxy = WebSocketProxy(websocket, WS_TARGET_ENDPOINT)
    await ws_proxy()
```

上述代码会将**客户端发送的消息**转发至**目标 WebSocket 服务器**，  
同时将**目标服务器的响应**转发回客户端。

现在假设 WebSocket 服务端的代码与之前的示例相同，  
那么 API 网关的代码如下：

```python
from distributed_websocket import WebSocketProxy
# 省略其他导入

app = FastAPI()

WS_TARGET_ENDPOINT = 'ws://websocket_service:8000/ws/{}'

@app.websocket('/ws/{conn_id}')
async def websocket_endpoint(
    ws: WebSocket,
    conn_id: str,
) -> None:
    await websocket.accept()
    ws_proxy = WebSocketProxy(websocket, WS_TARGET_ENDPOINT.format(conn_id))
    await ws_proxy()
```

## API 参考

### Connection（连接）

`Connection` 对象封装了 WebSocket 连接，并提供了简洁的接口来**发送**和**接收**消息。  
它们具有 `topics` 属性，可用于存储**订阅模式**，并实现 **发布/订阅（pub/sub）** 模型。

* **`async`** `accept(self) -> None`  
  接受 WebSocket 连接。
* **`async`** `close(self, code: int = 1000) -> None`  
  以指定的状态码关闭连接。
* **`async`** `receive_json(self) -> Any`  
  接收 JSON 格式的消息。
* **`async`** `send_json(self, data: Any) -> None`  
  通过 WebSocket 连接发送 JSON 消息。
* **`async`** `iter_json(self) -> AsyncIterator[Any]`  
  以异步迭代方式接收 WebSocket 消息。

---

### Messages（消息）

`Message` 对象用于存储**消息类型**、**主题（topic）**以及**数据（data）**，  
并提供了便捷的**序列化/反序列化**方法。  

**注意：** 通过 `connection.iter_json` 返回的消息已经被解析为 `dict` 对象。  
在此，我们的 *反序列化* 过程是指将 `dict` 转换为 `Message` 对象。

* `type: str`  
  消息类型。
* `topic: str`  
  消息所属的主题（topic）。
* `conn_id: str | list[str]`  
  目标连接 ID，或目标连接 ID 列表（如果要向多个连接发送）。
* `data: Any`  
  消息数据。

* **`classmethod`** `from_client_message(cls, *, data: Any) -> Message`  
  从客户端消息创建 `Message` 对象。
* `__serialize__(self) -> dict`  
  将 `Message` 对象序列化为 `dict`。

---

### Subscriptions（订阅）

可以将**主题（topics）**绑定到 `Connection` 对象，以实现**发布/订阅（pub/sub）**模型或**通知**功能。  
`topics` 属性是一个**字符串集合（set）**，采用与 MQTT 主题匹配规则类似的模式。

本库在**多个服务器实例**之间**共享** `Connection` 对象的状态，因此你可能会看到 `channel`、`publish`、`subscribe`、`unsubscribe` 等术语。  
它们用于描述服务器与**消息代理（broker）**之间的通信，而不是服务器与**客户端**的通信。  
请注意**区分**这两者概念，前者通常无需你直接处理。

* `subscribe(connection: Connection, message: Message) -> None`  
  订阅 `connection` 到 `message.topic`。
* `unsubscribe(connection: Connection, message: Message) -> None`  
  取消 `connection` 对 `message.topic` 的订阅。
* `handle_subscription_message(connection: Connection, message: Message) -> None`  
  根据消息类型调用 `subscribe` 或 `unsubscribe`。
* `matches(topic: str, patterns: set[str]) -> bool`  
  检查 `topic` 是否匹配 `patterns` 集合中的某个模式。

---

### Authentication（认证）

`fastapi-distributed-websocket` 提供了 `WebSocketOAuth2PasswordBearer` 进行**WebSocket 认证**。  
该类继承自 *FastAPI* 的 `OAuth2PasswordBearer`，并重写了 `__call__` 方法，以支持 `WebSocket` 对象。

* **`async`** `__call__(self, websocket: WebSocket) -> str | None`  
  **验证 WebSocket 连接**，并返回 `Authorization` 头的值。  
  如果认证失败：
    * 若 `auto_error=False`，则返回 `None`。
    * 否则，关闭 WebSocket 连接，并返回 `WS_1008_POLICY_VIOLATION` 状态码。

---

### Exceptions and Exception Handling（异常与异常处理）

`fastapi-distributed-websocket` 提供了基于**装饰器**的异常处理机制。  
你可以使用专门的装饰器，传入**异常类**和**异常处理函数**。  
异常处理函数只需要接受**异常对象**作为参数。

**为什么这很有用？**  
在应用的不同部分，可能会抛出**相同类型的异常**。  
使用装饰器，你可以在**调用栈的更高层**统一处理异常，而无需在各个地方单独处理。

此外，库内提供了 `WebSocketException` 基类，允许你**绑定** `Connection` 对象到异常。  
这样，在异常处理函数内部，你可以轻松访问引发异常的 WebSocket 连接。  
如果你希望异常处理函数能访问 `Connection` 对象，应让你的自定义异常**继承** `WebSocketException`，  
即使该异常本质上**并非** WebSocket 相关。

---

#### 主要异常类

* `WebSocketException(self, message: str, *, connection: Connection) -> None`  
  基础 WebSocket 异常，允许访问 `Connection` 对象。
* `InvalidSubscription(self, message: str, *, connection: Connection) -> None`  
  **订阅模式无效**时抛出，继承自 `WebSocketException`。
* `InvalidSubscriptionMessage(self, message: str, *, connection: Connection) -> None`  
  类似于 `InvalidSubscription`，但也可能在消息类型**不是** `subscribe` 或 `unsubscribe` 时抛出。

---

#### 主要异常处理装饰器

* `handle(exc: BaseException, handler: Callable[..., Any]) -> Callable[..., Any]`  
  **装饰器**：同步异常处理。  
  当 `exc` 类型的异常在被装饰的函数内抛出或传播到函数时，`handler` 负责处理。  
  仅当**装饰的函数**和**异常处理函数**都是**同步**时，才使用此装饰器。

* **`async`** `ahandle(
    exc: BaseException, handler: Callable[..., Coroutine[Any, Any, Any]]
) -> Callable[..., Any]`  
  **装饰器**：异步异常处理。  
  与 `handle` 类似，但 `handler` 需要是**协程函数（coroutine function）**。  
  适用于：
    * **异常处理函数**是异步的。
    * **被装饰的函数**可以是**同步**或**异步**。

### Broker 接口

服务器实例间的连接状态通过 **发布/订阅（pub/sub）** 代理共享。  
默认情况下，代理（broker）是一个 `redis.asyncio.Redis` 实例（之前是 `aioredis.Redis`），但你也可以使用**其他实现**。  
`fastapi-distributed-websocket` 提供了 `InMemoryBroker` 供开发测试使用。  
你可以继承 `BrokerInterface` 并**重写**相关方法，以实现自定义代理。

* **`async`** `connect(self) -> Coroutine[Any, Any, None]`  
  连接到代理（broker）。
* **`async`** `disconnect(self) -> Coroutine[Any, Any, None]`  
  断开与代理的连接。
* **`async`** `subscribe(self, channel: str) -> Coroutine[Any, Any, None]`  
  订阅一个频道（channel）。
* **`async`** `unsubscribe(self, channel: str) -> Coroutine[Any, Any, None]`  
  取消订阅一个频道。
* **`async`** `publish(self, channel: str, message: Any) -> Coroutine[Any, Any, None]`  
  向频道发布一条消息。
* **`async`** `get_message(self, **kwargs) -> Coroutine[Any, Any, Message | None]`  
  从代理获取一条消息。

---

### WebSocketManager（WebSocket 管理器）

`WebSocketManager` 是该库的核心逻辑所在。  
它用于**管理连接对象**，并**启动代理连接**。  

该类会创建一个**主要任务（main task）**，  
即一个**非阻塞监听器**，用于从代理接收消息，并将其发送到**连接对象**。  
（可以是**广播**，也可以检查**订阅**）  
对于每次发送消息，它都会**创建一个新的任务**进行处理。  

代理的初始化在 `__init__` 方法中完成，  
而 `broker.connect` 和 `broker.disconnect` 分别在 `startup` 和 `shutdown` 方法中调用。

---

#### 连接管理

* **`async`** `new_connection(self, websocket: WebSocket, conn_id: str, topic: str | None = None) -> Coroutine[Any, Any, Connection]`  
  **创建新连接对象**，并将其添加到 `self.active_connections` 中，返回该连接对象。
* **`async`** `close_connection(self, connection: Connection, code: int = status.WS_1000_NORMAL_CLOSURE) -> Coroutine[Any, Any, None]`  
  **关闭连接对象**，并从 `self.active_connections` 中移除。
* `remove_connection(self, connection: Connection) -> None`  
  从 `self.active_connections` 中移除指定的连接对象。
* `set_conn_id(self, connection: Connection, conn_id: str) -> None`  
  **设置连接 ID**，并通知客户端。

---

#### 消息发送

* `send(self, message: Message) -> None`  
  **向所有订阅 `message.topic` 的连接对象发送消息**。  
  此操作会**创建一个新任务**，以执行 `self._send` 生成的协程。
* `broadcast(self, message: Message) -> None`  
  **向所有连接对象发送消息（广播）**。  
  此操作会**创建一个新任务**，以执行 `self._broadcast` 生成的协程。
* `send_by_conn_id(self, message: Message) -> None`  
  **向指定连接 ID（`message.conn_id`）发送消息**。  
    * 如果 `conn_id` 是字符串，则**创建一个新任务**，执行 `self._send_by_conn_id` 生成的协程。  
    * 如果 `conn_id` 是列表，则执行 `_send_multi_by_conn_id` 生成的协程。
* `send_msg(self, message: Message) -> None`  
  **根据消息类型**调用 `send`、`send_by_conn_id` 或 `broadcast`。

---

#### 消息接收

* **`async`** `receive(self, connection: Connection, message: Any) -> Coroutine[Any, Any, None]`  
  **从连接对象接收消息**，并传递给私有方法处理**订阅逻辑**，  
  然后将消息发布到**代理**。

---

#### 生命周期管理

* **`async`** `startup(self) -> Coroutine[Any, Any, None]`  
  **启动代理连接**，并**启动监听任务**。
* **`async`** `shutdown(self) -> Coroutine[Any, Any, None]`  
  **关闭代理连接**，并**关闭监听任务**。  
  同时：
    * **取消** `send_msg` 生成的所有任务。
    * **关闭**所有连接对象。

---

### WebSocketProxy（WebSocket 代理）

`WebSocketProxy` 允许**代理 WebSocket 消息**，  
将**客户端消息**转发到**服务器**，并将**服务器消息**转发回**客户端**。  
它的初始化需要两个参数：

* **client**：`WebSocket` 对象（客户端连接）。
* **server_endpoint**：目标服务器的**URL**。

目标服务器可以是**远程服务器**，  
也可以是**本地服务器**（即启动代理的服务器本身）。

* **`async`** `__call__(self) -> Coroutine[Any, Any, None]`  
  **启动 WebSocket 连接到 `server_endpoint`**，  
  并**创建两个任务**：
    * **任务 1**：将**客户端消息**转发到**目标服务器**。
    * **任务 2**：将**目标服务器消息**转发回**客户端**。
