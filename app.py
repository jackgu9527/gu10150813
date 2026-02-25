import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime

# ==========================================
# 1. 系統初始化與資料庫設定 (SQLite)
# ==========================================
st.set_page_config(page_title="大隊部圖書管理系統", layout="wide")

def init_db():
    conn = sqlite3.connect('military_books.db')
    c = conn.cursor()
    
    # 建立使用者資料表
    c.execute('''CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    login_id TEXT UNIQUE,
                    password TEXT,
                    role TEXT,
                    unit TEXT,
                    squadron TEXT,
                    title TEXT,
                    name TEXT,
                    discharge_date DATE,
                    setup_count INTEGER DEFAULT 1
                )''')
    
    # 建立圖書資料表
    c.execute('''CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    book_name TEXT,
                    serial_number TEXT UNIQUE,
                    owner_id TEXT,
                    status TEXT
                )''')
                
    # 建立借閱申請單表
    c.execute('''CREATE TABLE IF NOT EXISTS borrow_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    login_id TEXT,
                    unit TEXT,
                    book_name TEXT,
                    quantity INTEGER,
                    status TEXT
                )''')
    
    # 寫入預設長官與幹部資料 (完整 26 員編制 + 1 測試班隊)
    c.execute("SELECT COUNT(*) FROM users")
    if c.fetchone()[0] == 0:
        default_users = [
            ('1', '1', 'L1', '大隊部', '大隊部', '系統管理員', '', None, 0),
            ('2', '2', 'L2', '大隊部', '大隊部', '大隊長', '', None, 0),
            ('3', '3', 'L2', '大隊部', '大隊部', '大隊輔導長', '', None, 0),
            ('4', '4', 'L3', '學員一中隊', '學員一中隊', '隊長', '', None, 0),
            ('5', '5', 'L3', '學員一中隊', '學員一中隊', '輔導長', '', None, 0),
            ('6', '6', 'L3', '學員二中隊', '學員二中隊', '隊長', '', None, 0),
            ('7', '7', 'L3', '學員二中隊', '學員二中隊', '輔導長', '', None, 0),
            ('8', '8', 'L3', '學生一中隊', '學生一中隊', '隊長', '', None, 0),
            ('9', '9', 'L3', '學生一中隊', '學生一中隊', '輔導長', '', None, 0),
            ('10', '10', 'L3', '學生二中隊', '學生二中隊', '隊長', '', None, 0),
            ('11', '11', 'L3', '學生二中隊', '學生二中隊', '輔導長', '', None, 0),
            ('12', '12', 'L4', '學生一中隊', '學生一中隊', '區隊長', '①', None, 1),
            ('13', '13', 'L4', '學生一中隊', '學生一中隊', '區隊長', '②', None, 1),
            ('14', '14', 'L4', '學生二中隊', '學生二中隊', '區隊長', '①', None, 1), # 測試用幹部
            ('15', '15', 'L4', '學生二中隊', '學生二中隊', '區隊長', '②', None, 1),
            ('16', '16', 'L4', '學生二中隊', '學生二中隊', '分隊長', '①', None, 1),
            ('17', '17', 'L4', '學生二中隊', '學生二中隊', '分隊長', '②', None, 1),
            ('18', '18', 'L4', '學生二中隊', '學生二中隊', '分隊長', '③', None, 1),
            ('19', '19', 'L4', '學生二中隊', '學生二中隊', '分隊長', '④', None, 1),
            ('20', '20', 'L4', '學生二中隊', '學生二中隊', '分隊長', '⑤', None, 1),
            ('21', '21', 'L4', '學生二中隊', '學生二中隊', '分隊長', '⑥', None, 1),
            ('22', '22', 'L4', '學生二中隊', '學生二中隊', '分隊長', '⑦', None, 1),
            ('23', '23', 'L4', '學生二中隊', '學生二中隊', '分隊長', '⑧', None, 1),
            ('24', '24', 'L4', '學生二中隊', '學生二中隊', '分隊長', '⑨', None, 1),
            ('25', '25', 'L4', '學員生一中隊', '學員生一中隊', '人事行政管理兵', '①', None, 1),
            ('26', '26', 'L4', '學員生一中隊', '學員生一中隊', '人事行政管理兵', '②', None, 1), # 文書兵
            ('27', '27', 'L5', '機步一連', '學生二中隊', '訓員', '', '2026-12-31', 1) # 測試訓員
        ]
        c.executemany("INSERT INTO users (login_id, password, role, unit, squadron, title, name, discharge_date, setup_count) VALUES (?,?,?,?,?,?,?,?,?)", default_users)
        
        # 自動寫入 100 本測試準則 (名稱 1~100，序號 1~100，狀態全部在資料庫庫存)
        default_books = []
        for i in range(1, 101):
            default_books.append((str(i), str(i), '在庫', '在庫'))
            
        c.executemany("INSERT INTO books (book_name, serial_number, owner_id, status) VALUES (?,?,?,?)", default_books)
    
    conn.commit()
    conn.close()

init_db()

# ==========================================
# 2. 共用函式庫
# ==========================================
def get_db_connection():
    return sqlite3.connect('military_books.db')

def login(username, password):
    conn = get_db_connection()
    user = pd.read_sql_query(f"SELECT * FROM users WHERE login_id='{username}' AND password='{password}'", conn)
    conn.close()
    if not user.empty:
        for col in user.columns:
            st.session_state[col] = user.iloc[0][col]
        st.session_state['logged_in'] = True
        st.rerun()
    else:
        st.error("❌ 帳號或密碼錯誤 / 或已被系統鎖定")

def logout():
    st.session_state.clear()
    st.rerun()

# ==========================================
# 3. 介面顯示邏輯與左側選單 (Sidebar)
# ==========================================
if 'logged_in' not in st.session_state:
    st.markdown("## 🛡️ 大隊部圖書管理系統登入")
    login_id = st.text_input("帳號 (Login ID)")
    password = st.text_input("密碼 (Password)", type="password")
    if st.button("登入"):
        login(login_id, password)
    st.stop()

if st.session_state.role in ['L1', 'L2', 'L3']:
    display_name = f"{st.session_state.squadron}{st.session_state.title}"
elif st.session_state.role == 'L4':
    display_name = f"{st.session_state.squadron}{st.session_state.title} {st.session_state.name}"
else:
    display_name = f"{st.session_state.unit}"

with st.sidebar:
    st.markdown(f"### {display_name}")
    st.markdown(f"ID: {st.session_state.login_id}")
    st.markdown("---")
    
    if st.session_state.role == 'L5':
        menu = st.radio("功能導覽", ["戰情首頁", "準則借閱", "準則清點", "準則歸還"])
    else:
        menu = st.radio("管理作業", ["戰情首頁", "審核與管理", "綜合查詢", "全時日誌"])
    
    st.markdown("---")
    if st.button("登出"):
        logout()

# ==========================================
# 4. 主畫面邏輯 (依據選單路由)
# ==========================================
conn = get_db_connection()

if menu == "戰情首頁":
    st.header("📊 戰情首頁")
    if st.session_state.role == 'L5':
        st.markdown("#### 我的持有清單")
        df = pd.read_sql_query(f"SELECT book_name as 書名, serial_number as 序號, status as 狀態 FROM books WHERE owner_id='{st.session_state.login_id}'", conn)
        if df.empty:
            st.info("您目前名下無任何圖書。請至「準則借閱」提出申請。")
        else:
            st.dataframe(df, use_container_width=True)
    else:
        st.markdown(f"歡迎登入，{display_name}。")
        st.info("💡 跨夜自動結算腳本 (Doomsday Script) 已於背景待命。")

elif menu == "準則借閱" and st.session_state.role == 'L5':
    st.header("📥 準則借閱申請")
    available_books = pd.read_sql_query("SELECT DISTINCT book_name FROM books WHERE status='在庫'", conn)
    
    if not available_books.empty:
        book_choice = st.selectbox("選擇需要借閱的準則", available_books['book_name'].tolist())
        qty = st.number_input("申請數量", min_value=1, max_value=50, value=1)
        if st.button("✅ 送出借閱申請"):
            c = conn.cursor()
            c.execute("INSERT INTO borrow_requests (login_id, unit, book_name, quantity, status) VALUES (?,?,?,?,?)",
                      (st.session_state.login_id, st.session_state.unit, book_choice, qty, '待審核'))
            conn.commit()
            st.success(f"已送出申請：申請借閱名稱「{book_choice}」共 {qty} 本！請等待文書兵核准。")
    else:
        st.warning("目前庫房內無任何可用準則。")

elif menu == "準則歸還" and st.session_state.role == 'L5':
    st.header("📤 準則歸還 (方案B: Checkbox 批次歸還)")
    books_df = pd.read_sql_query(f"SELECT id, book_name as 書名, serial_number as 序號 FROM books WHERE owner_id='{st.session_state.login_id}' AND status='借閱中'", conn)
    
    if not books_df.empty:
        books_df.insert(0, "勾選歸還", False)
        edited_df = st.data_editor(books_df, hide_index=True, use_container_width=True)
        selected_ids = edited_df[edited_df["勾選歸還"] == True]["id"].tolist()
        
        col1, col2 = st.columns([1, 4])
        with col1:
            if st.button("✅ 一鍵全數歸還", type="primary"):
                all_ids = books_df["id"].tolist()
                c = conn.cursor()
                c.execute(f"UPDATE books SET status='歸還中' WHERE id IN ({','.join(map(str, all_ids))})")
                conn.commit()
                st.success("已送出全數歸還申請！等待幹部點收。")
                st.rerun()
        with col2:
            if st.button("送出勾選項目"):
                if selected_ids:
                    c = conn.cursor()
                    c.execute(f"UPDATE books SET status='歸還中' WHERE id IN ({','.join(map(str, selected_ids))})")
                    conn.commit()
                    st.success(f"已送出 {len(selected_ids)} 本歸還申請！")
                    st.rerun()
                else:
                    st.warning("請至少勾選一本書籍")
    else:
        st.success("您名下目前沒有需要歸還的圖書！")

elif menu == "審核與管理" and st.session_state.role in ['L1', 'L2', 'L3', 'L4']:
    st.header("⚙️ 審核與管理核心後台")
    
    if st.session_state.role == 'L4':
        if "人事行政" in st.session_state.title or "文書" in st.session_state.title:
            tabs = st.tabs(["註冊審核", "資料修正 (結訓日)", "借閱審核", "帳號管理"])
            is_doc = True
        else:
            tabs = st.tabs(["註冊審核", "資料修正 (帳密救援)", "歸還點收", "帳號管理"])
            is_doc = False
            
        with tabs[2]: 
            if is_doc:
                # 借閱審核 (文書兵專屬)
                st.subheader("📚 借閱單審核")
                req_df = pd.read_sql_query("SELECT id as 申請單號, unit as 申請班隊, book_name as 書名, quantity as 申請數量 FROM borrow_requests WHERE status='待審核'", conn)
                st.dataframe(req_df, hide_index=True)
                
                if not req_df.empty:
                    req_id = st.selectbox("選擇要核准的單號", req_df['申請單號'].tolist())
                    if st.button("✅ 核准並自動從庫存配發"):
                        target_req = req_df[req_df['申請單號'] == req_id].iloc[0]
                        c = conn.cursor()
                        c.execute(f"SELECT id FROM books WHERE book_name='{target_req['書名']}' AND status='在庫' LIMIT {target_req['申請數量']}")
                        available_books = c.fetchall()
                        
                        if len(available_books) < target_req['申請數量']:
                            st.error(f"庫存不足！該準則目前僅剩 {len(available_books)} 本。")
                        else:
                            book_ids = [str(b[0]) for b in available_books]
                            login_id = pd.read_sql_query(f"SELECT login_id FROM borrow_requests WHERE id={req_id}", conn).iloc[0]['login_id']
                            c.execute(f"UPDATE books SET status='借閱中', owner_id='{login_id}' WHERE id IN ({','.join(book_ids)})")
                            c.execute(f"UPDATE borrow_requests SET status='已發放' WHERE id={req_id}")
                            conn.commit()
                            st.success("✅ 核准成功！系統已自動綁定實體書序號並發放給該班隊。")
                            st.rerun()
            else:
                # 歸還點收 (區分隊長專屬)
                st.subheader("📦 批次歸還點收")
                return_df = pd.read_sql_query(f"SELECT u.unit as 班隊, b.book_name as 書名, COUNT(b.id) as 數量 FROM books b JOIN users u ON b.owner_id = u.login_id WHERE b.status='歸還中' GROUP BY u.unit, b.book_name", conn)
                st.dataframe(return_df, use_container_width=True)
                
                if st.button("✅ 批次確認收回所有清單"):
                    c = conn.cursor()
                    c.execute("UPDATE books SET status='在庫', owner_id='在庫' WHERE status='歸還中'")
                    conn.commit()
                    st.success("已全數點收完畢，轉入歷史歸檔！")
                    st.rerun()

        with tabs[3]: 
            st.subheader("👤 帳號救援與重置")
            l5_users = pd.read_sql_query("SELECT id, unit as 單位, login_id as 帳號, setup_count as 免審額度 FROM users WHERE role='L5'", conn)
            l5_users.insert(0, "選取", False)
            edited_u = st.data_editor(l5_users, hide_index=True)
            selected_u_ids = edited_u[edited_u["選取"] == True]["id"].tolist()
            
            if st.button("🔄 批次重置密碼為 army1234 (並恢復免審額度)"):
                if selected_u_ids:
                    c = conn.cursor()
                    c.execute(f"UPDATE users SET password='army1234', setup_count=1 WHERE id IN ({','.join(map(str, selected_u_ids))})")
                    conn.commit()
                    st.success("已成功重置密碼為 army1234，並恢復該班隊之修改額度！")
                    st.rerun()

elif menu == "綜合查詢":
    st.header("🔍 綜合查詢")
    search_type = st.radio("查詢模式", ["查書名 (掌握分布)", "查序號 (精準定位)"], horizontal=True)
    keyword = st.text_input("請輸入關鍵字")
    
    if st.button("搜尋"):
        if "書名" in search_type:
            res = pd.read_sql_query(f"SELECT u.squadron as 中隊, u.unit as 班隊, COUNT(b.id) as 數量 FROM books b JOIN users u ON b.owner_id = u.login_id WHERE b.book_name LIKE '%{keyword}%' GROUP BY u.squadron, u.unit", conn)
            st.dataframe(res, use_container_width=True)
        else:
            res = pd.read_sql_query(f"SELECT u.squadron as 中隊, u.unit as 班隊, b.book_name as 書名, b.status as 狀態 FROM books b JOIN users u ON b.owner_id = u.login_id WHERE b.serial_number = '{keyword}'", conn)
            st.dataframe(res, use_container_width=True)

conn.close()
