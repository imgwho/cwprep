---
description: 发布流程管理 (暗号：/publish 发布到 PyPI, /commit 仅提交 GitHub)
---

### 🚀 模式 A：执行完整发布 (/publish)
当用户输入 `/publish`（或“执行发布”、“执行光速发布”）时：

1. 检查 `pyproject.toml` 中的版本号，确保它比上一版本新（如果未修改则提示用户）。然后更新所有文档，包括readme，changelog等。
2. 将当前所有更改添加到 Git 中（`git add .`）。
3. 提交一个英文 Commit，内容总结当次的改动（如：`git commit -m "feat: Add new feature"`）。
4. 推送代码到 GitHub：`git push origin master`。
5. 运行打包命令：`python -m build`。
6. 使用 `twine` 上传到 PyPI：读取 `.env` 里的密钥，执行 `twine upload --non-interactive -u "__token__" -p "YOUR_TOKEN" dist/*`。
// turbo-all
7. 完成后告知用户发布成功及 PyPI 链接。

---

### 📝 模式 B：仅提交代码 (/commit)
当用户输入 `/commit` 时：

1. 将当前所有更改添加到 Git 中（`git add .`）。
2. 提交一个英文 Commit，内容总结当次的改动。
3. 推送代码到 GitHub：`git push origin master`。
// turbo-all
4. 完成后确认提交成功。