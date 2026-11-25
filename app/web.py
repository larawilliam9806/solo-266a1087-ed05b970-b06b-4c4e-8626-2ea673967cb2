"""Flask 报送页：按月份、资源号、馆号上传 JSON，支持换报、撤回与下载。"""

from __future__ import annotations

import json
import os
from typing import Any

from flask import (
    Flask,
    Response,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from .db import Database
from .errors import NotFoundError, ValidationError


def create_app(db_path: str | None = None) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path or os.environ.get(
        "HLL_DB_PATH", "/data/hll.sqlite3"
    )
    app.config["DATABASE"] = Database(app.config["DB_PATH"])

    def get_db() -> Database:
        return app.config["DATABASE"]

    # -- 入参解析 --------------------------------------------------------

    def _wants_html() -> bool:
        """浏览器表单提交走 HTML 跳转，JSON/API 调用一律走 JSON 响应。"""
        if request.path.endswith("/download"):
            return False
        # 只有浏览器表单（urlencoded / multipart）才走页面跳转；
        # application/json 及其他程序化调用（含 Accept: */* 的客户端）返回 JSON。
        return request.content_type is not None and (
            request.content_type.startswith("application/x-www-form-urlencoded")
            or request.content_type.startswith("multipart/form-data")
        )

    def _payload() -> dict[str, Any]:
        """同时支持 application/json、multipart/form-data 与 JSON 文件上传。"""
        if request.is_json:
            data = request.get_json(silent=False)
            if not isinstance(data, dict):
                raise ValidationError("JSON 请求体必须是对象")
            return data

        data: dict[str, Any] = {}
        if request.form:
            data.update(request.form.to_dict(flat=True))
        upload = request.files.get("file")
        if upload is not None and upload.filename:
            try:
                uploaded = json.loads(upload.read().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError(f"上传的文件不是合法 JSON：{exc}") from exc
            if not isinstance(uploaded, dict):
                raise ValidationError("上传的 JSON 文件内容必须是对象")
            for key, value in uploaded.items():
                data.setdefault(key, value)
        if request.data and not data:
            try:
                raw = json.loads(request.data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValidationError(f"请求体不是合法 JSON：{exc}") from exc
            if not isinstance(raw, dict):
                raise ValidationError("JSON 请求体必须是对象")
            data = raw
        return data

    def _field(data: dict[str, Any], key: str) -> Any:
        value = data.get(key)
        if value is None or (isinstance(value, str) and not value.strip()):
            raise ValidationError(f"缺少必填字段 {key}")
        return value

    # -- 错误处理 --------------------------------------------------------

    @app.errorhandler(ValidationError)
    @app.errorhandler(NotFoundError)
    def _handle_known_error(err: Exception) -> Response:
        message = getattr(err, "message", str(err))
        status = getattr(err, "status", 400)
        if _wants_html():
            return redirect(url_for("index", error=message, status=status))
        response = jsonify({"error": message})
        response.status_code = status
        return response

    # -- 页面 ------------------------------------------------------------

    @app.get("/")
    def index() -> str:
        return render_template(
            "index.html",
            error=request.args.get("error"),
            status=request.args.get("status"),
            message=request.args.get("message"),
        )

    @app.get("/health")
    def health() -> Response:
        return jsonify({"status": "ok"})

    # -- 报送 / 换报 -----------------------------------------------------

    @app.post("/reports")
    def submit_report() -> Response:
        data = _payload()
        month = _field(data, "month")
        resource = _field(data, "resource")
        branch = _field(data, "branch")
        registers = _field(data, "registers")

        result = get_db().upsert_report(month, resource, branch, registers)
        if _wants_html():
            action = "换报" if result["replaced"] else "报送"
            return redirect(
                url_for(
                    "index",
                    status=0,
                    message=f"馆 {branch} {action}成功，合并摘要已同步更新",
                )
            )
        response = jsonify(result)
        response.status_code = 200 if result["replaced"] else 201
        return response

    # -- 撤回 ------------------------------------------------------------

    @app.route("/reports/withdraw", methods=["POST", "DELETE"])
    def withdraw_report() -> Response:
        if request.is_json:
            data = request.get_json(silent=False)
            if not isinstance(data, dict):
                raise ValidationError("JSON 请求体必须是对象")
        else:
            data = request.form.to_dict(flat=True) or request.args.to_dict(flat=True)
        month = _field(data, "month")
        resource = _field(data, "resource")
        branch = _field(data, "branch")

        result = get_db().withdraw_report(month, resource, branch)
        if _wants_html():
            return redirect(
                url_for(
                    "index",
                    status=0,
                    message=f"馆 {branch} 已撤回 {month} 资源 {resource} 的报送",
                )
            )
        return jsonify(result)

    # -- 查询本馆现行报送 -------------------------------------------------

    @app.get("/reports/<month>/<resource>/<branch>")
    def get_report(month: str, resource: str, branch: str) -> Response:
        return jsonify(get_db().get_report(month, resource, branch))

    # -- 下载合并摘要 -----------------------------------------------------

    @app.get("/merged/<month>/<resource>/download")
    def download_merged(month: str, resource: str) -> Response:
        result = get_db().get_merged(month, resource)
        body = json.dumps(result, ensure_ascii=False, indent=2)
        filename = f"merged_{month}_{resource}.json"
        return Response(
            body,
            mimetype="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )

    return app
