# Spec-Driven 含义 + 简易 Demo
## 一、核心释义
**Spec-Driven = 规约驱动 / 规范驱动开发**
- Spec = Specification（需求规约、接口规范、数据协议、API文档、产品定义文档）
- 核心思想：**先写规范文档，再写代码；规范是唯一可信源，代码自动对齐规范**
- 对比：
  1. Code-Driven（代码驱动）：先写代码，后补文档，文档容易滞后、不一致
  2. Spec-Driven（规约驱动）：Spec 为第一源头，代码、测试、Mock、SDK 全部由 Spec 生成/校验

### 常见落地场景
1. API：OpenAPI/Swagger 先定义接口 Spec，生成后端骨架、前端请求、Mock、自动化测试
2. 微服务：Protobuf IDL、GraphQL Schema
3. 数据校验：JSON Schema 定义数据结构，代码强制校验入参出参
4. 测试：Spec 定义预期行为，自动生成测试用例（BDD、契约测试）

## 二、Demo1：OpenAPI Spec-Driven API（最常用）
### 步骤1：先写 Spec（spec.yaml，源头）
```yaml
openapi: 3.0.0
info:
  title: 用户服务规约
  version: 1.0.0
paths:
  /user/{id}:
    get:
      summary: 根据ID获取用户
      parameters:
        - name: id
          in: path
          required: true
          schema:
            type: integer
      responses:
        200:
          description: 返回用户信息
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/User"
components:
  schemas:
    User:
      type: object
      properties:
        id: integer
        name: string
        age: integer
```

### 步骤2：由 Spec 自动产出代码（Spec-Driven 核心动作）
使用工具 `openapi-generator`：
```bash
# 根据 spec 自动生成 Go/Java/Python/TS 后端接口骨架、前端请求代码
openapi-generator generate -i spec.yaml -g typescript-axios -o ./api-client
```
产出物：
- 前端 TS 请求函数
- 数据类型定义
- Mock 服务
- 接口自动化测试用例

### 开发逻辑（Spec 优先）
1. 产品/后端对齐，写完 OpenAPI Spec
2. 前端直接用生成好的请求代码开发页面
3. 后端基于生成的路由/结构体实现业务逻辑
4. 契约测试直接基于 Spec 校验前后端一致性

## 三、Demo2：JSON Schema Spec-Driven 数据校验
### Step1：定义 Spec（data-spec.json，数据规范）
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "title": "订单规范",
  "required": ["orderId", "amount"],
  "properties": {
    "orderId": {"type": "string"},
    "amount": {"type": "number", "minimum": 0}
  }
}
```

### Step2：代码基于 Spec 校验（Python demo）
```python
from jsonschema import validate
import json

# 1. Spec 作为唯一标准
order_spec = json.load(open("data-spec.json"))

# 合法数据
valid_order = {"orderId": "OD123", "amount": 99.9}
# 非法数据（金额负数，违反Spec）
invalid_order = {"orderId": "OD456", "amount": -10}

validate(instance=valid_order, schema=order_spec)  # 通过
validate(instance=invalid_order, schema=order_spec) # 抛出校验异常
```
优势：修改数据结构只改 Spec，不用到处改校验代码。

## 四、Demo3：Protobuf Spec-Driven 微服务
### Step1：先写 IDL Spec（user.proto）
```protobuf
syntax = "proto3";
service UserService {
  rpc GetUser(GetUserReq) returns (UserResp);
}
message GetUserReq {
  int32 id = 1;
}
message UserResp {
  int32 id = 1;
  string name = 2;
}
```

### Step2：编译 Spec 生成服务代码
```bash
# 基于 proto spec 自动生成 Go/Java/C++ 服务桩代码
protoc --go_out=. --go-grpc_out=. user.proto
```
业务开发只需要实现生成好的接口，**数据结构、序列化规则完全由 Spec 控制**。

## 五、Spec-Driven 优缺点
### 优点
1. 前后端、跨团队统一标准，无口头需求歧义
2. Spec 可自动生成代码、文档、Mock、测试，减少重复劳动
3. 变更只维护一份 Spec，避免代码与文档不同步
4. 适合大型团队、标准化平台、开放API

### 缺点
1. 前期需要投入时间完善 Spec，小型快速原型项目略显繁琐
2. 复杂动态逻辑很难全部在 Spec 中描述，仍需补充业务代码

## 六、配套常见术语区分
1. Spec-Driven Development (SDD)：规约驱动开发
2. API-First：本质就是 Spec-Driven 在接口领域的实践
3. Contract Testing（契约测试）：Spec 就是服务间契约
4. Code-First：反向，先写代码再导出 Spec（不属于 Spec-Driven）