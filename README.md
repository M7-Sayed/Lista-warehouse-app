# معالج الليسته 🏭

نظام لمعالجة ليسته المخازن وإدارة الكوتات.

## المميزات
- رفع الليسته واختيار المخزن
- حفظ كوتة كل مخزن تلقائياً
- لو المخزن بعت ليسته بدون كوتة، بيستخدم الكوتة المحفوظة
- تاريخ كامل لكل كوتة
- يشتغل على أي جهاز في الفريق

## الرفع على Railway

1. عمل حساب على [railway.app](https://railway.app)
2. اعمل New Project → Deploy from GitHub repo
3. ارفع الكود على GitHub وربطه
4. أو من Railway CLI:
   ```
   railway login
   railway init
   railway up
   ```

## التشغيل محلياً

```bash
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

افتح: http://localhost:8000
