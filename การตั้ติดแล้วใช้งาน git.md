# การสร้างหรือโคลนโปรเจคจาก GitHub
หลังจากตั้งค่า Git และ GitHub เรียบร้อยแล้ว คุณสามารถเริ่มใช้งาน GitHub ในการสร้างโปรเจคใหม่หรือโคลนโปรเจคที่มีอยู่แล้ว

### การสร้างโปรเจคใหม่:
1. ไปที่ GitHub และคลิกที่ New ที่หน้า Dashboard
2. ตั้งชื่อโปรเจคและเลือกว่าจะให้เป็น Public หรือ Private
3. คลิก Create repository
### การโคลนโปรเจคจาก GitHub:
```bash
git clone git@github.com:username/repository_name.git
```

4. สร้าง SSH Key (หากยังไม่มี) โดยใช้คำสั่ง
```bash
ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
```

5. เพิ่ม SSH Key ใน GitHub:
- ไปที่ GitHub -> Settings -> SSH and GPG keys -> New SSH Key
- คัดลอก public key ที่คุณได้สร้างแล้ว (~/.ssh/id_rsa.pub) และวางใน GitHub
  
1. ตรวจสอบการเชื่อมต่อ SSH:
```bash
ssh -T git@github.com
```

1. เปลี่ยน URL ของ repository เป็น SSH: ใช้คำสั่งนี้เพื่อเปลี่ยน URL ของ repository:
```bash
git remote set-url origin git@github.com:sephirothlab/myDataWarehouse.git
```

# ทำงานกับ Git: การ Commit และ Push การเปลี่ยนแปลง
### การเพิ่มไฟล์ใหม่:
เมื่อทำการเปลี่ยนแปลงหรือเพิ่มไฟล์ในโปรเจคของคุณ, ใช้คำสั่งนี้เพื่อเพิ่มไฟล์ที่เปลี่ยนแปลง:
```bash
git add .
```

### การ Commit การเปลี่ยนแปลง:
หลังจากเพิ่มไฟล์แล้ว, ทำการ commit การเปลี่ยนแปลง:
```bash
git commit -m "Your commit message"
```

### การ Push การเปลี่ยนแปลงไปยัง GitHub:
เมื่อ commit เสร็จแล้ว, ใช้คำสั่งนี้เพื่อส่งการเปลี่ยนแปลงไปยัง GitHub:
```bash
git push origin main
```