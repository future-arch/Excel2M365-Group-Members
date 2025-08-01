import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
import pandas as pd
import requests
import msal
import json
import threading
import queue
import os
import logging
from urllib.parse import quote
import time
from dotenv import load_dotenv  # Used to load environment variables from a .env file

# Load environment variables from .env file
load_dotenv()

# --- 配置区域 ---
# 安全配置：从环境变量读取凭据
CLIENT_ID = os.getenv('AZURE_CLIENT_ID', '')
CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET', '')
TENANT_ID = os.getenv('AZURE_TENANT_ID', '')

# 验证必需的环境变量
if not all([CLIENT_ID, CLIENT_SECRET, TENANT_ID]):
    missing_vars = []
    if not CLIENT_ID: missing_vars.append('AZURE_CLIENT_ID')
    if not CLIENT_SECRET: missing_vars.append('AZURE_CLIENT_SECRET')
    if not TENANT_ID: missing_vars.append('AZURE_TENANT_ID')
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# --- 全局常量 ---
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
GRAPH_ENDPOINT = "https://graph.microsoft.com/v1.0"
SCOPES = ["https://graph.microsoft.com/.default"]
MAX_RETRIES = 3
RETRY_DELAY = 1
REQUEST_TIMEOUT = 30

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# 核心 API 服务类 (后端逻辑)
# ==============================================================================
class GraphAPI:
    """封装所有与 Microsoft Graph API 的交互，包含完整的错误处理"""
    def __init__(self):
        self.access_token = None
        self.token_expires_at = 0
        self._session = requests.Session()
        self._session.timeout = REQUEST_TIMEOUT

    def get_access_token(self):
        """获取访问令牌，支持缓存与重试"""
        current_time = time.time()
        if self.access_token and current_time < self.token_expires_at:
            return True
            
        for attempt in range(MAX_RETRIES):
            try:
                app = msal.ConfidentialClientApplication(
                    CLIENT_ID, authority=AUTHORITY, client_credential=CLIENT_SECRET
                )
                result = app.acquire_token_for_client(scopes=SCOPES)
                if "access_token" in result:
                    self.access_token = result['access_token']
                    # 设置令牌过期时间（提前5分钟失效）
                    self.token_expires_at = current_time + result.get('expires_in', 3600) - 300
                    logger.info("成功获取访问令牌")
                    return True
                else:
                    error_msg = result.get('error_description', '未知错误')
                    logger.error(f"无法获取访问令牌: {error_msg}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                    return False
            except Exception as e:
                logger.error(f"获取访问令牌时发生异常 (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                return False
        return False

    def _get_headers(self):
        if not self.access_token:
            raise ValueError("访问令牌为空")
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'ConsistencyLevel': 'eventual'
        }
        
    def _make_request(self, method, url, **kwargs):
        """带重试机制的HTTP请求"""
        for attempt in range(MAX_RETRIES):
            try:
                response = self._session.request(method, url, headers=self._get_headers(), **kwargs)
                if response.status_code == 429:  # Rate limited
                    retry_after = int(response.headers.get('Retry-After', RETRY_DELAY))
                    logger.warning(f"被限流，{retry_after}秒后重试")
                    time.sleep(retry_after)
                    continue
                elif response.status_code == 401:  # Token expired
                    logger.info("令牌过期，重新获取")
                    if self.get_access_token():
                        continue
                    else:
                        raise requests.exceptions.HTTPError("Unable to refresh token")
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                    continue
                raise
        raise requests.exceptions.RequestException(f"Failed after {MAX_RETRIES} attempts")

    def get_all_groups(self):
        """获取所有 M365 组和安全组"""
        groups = []
        url = f"{GRAPH_ENDPOINT}/groups?$select=id,displayName,groupTypes,mailEnabled,securityEnabled"
        
        try:
            while url:
                response = self._make_request('GET', url)
                data = response.json()
                groups.extend(data.get('value', []))
                url = data.get('@odata.nextLink')
                
            logger.info(f"成功获取 {len(groups)} 个组")
            return groups
        except Exception as e:
            logger.error(f"获取组列表时发生异常: {e}")
            return None

    def check_user_exists(self, upn):
        """通过 UPN 或邮件地址检查用户是否存在"""
        if not upn or not upn.strip():
            return None, "用户名为空"
            
        # 输入验证和清理
        upn = upn.strip().lower()
        if '@' not in upn:
            return None, "无效的邮件格式"
            
        try:
            # URL编码防止注入
            encoded_upn = quote(upn, safe='')
            filter_query = f"$filter=userPrincipalName eq '{encoded_upn}' or mail eq '{encoded_upn}'"
            select_fields = "$select=id,userPrincipalName,displayName,givenName,surname,companyName,department,jobTitle,mobilePhone,mail,userType"
            url = f"{GRAPH_ENDPOINT}/users?{filter_query}&{select_fields}"
            
            response = self._make_request('GET', url)
            data = response.json()
            
            if data.get('value'):
                return data['value'][0], "用户已存在"
            else:
                return None, "用户不存在"
                
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None, "用户不存在"
            logger.error(f"HTTP错误检查用户 {upn}: {e.response.status_code} - {e.response.text}")
            return None, f"错误: {e.response.status_code}"
        except Exception as e:
            logger.error(f"检查用户存在时发生异常: {e}")
            return None, f"网络错误: {str(e)}"

    def invite_guest_user(self, email, display_name):
        """邀请外部用户成为来宾"""
        if not email or not email.strip():
            return None, "邮件地址为空"
            
        email = email.strip().lower()
        if '@' not in email:
            return None, "无效的邮件格式"
            
        display_name = display_name.strip() if display_name else email.split('@')[0]
        
        payload = {
            "invitedUserEmailAddress": email, 
            "inviteRedirectUrl": "https://myapps.microsoft.com", 
            "invitedUserDisplayName": display_name, 
            "sendInvitationMessage": True
        }
        
        try:
            response = self._make_request('POST', f"{GRAPH_ENDPOINT}/invitations", json=payload)
            if response.status_code == 201:
                logger.info(f"成功邀请用户: {email}")
                return response.json().get('invitedUser'), "邀请成功"
            else:
                logger.error(f"邀请用户失败: {response.status_code} - {response.text}")
                return None, f"邀请失败: {response.status_code}"
        except Exception as e:
            logger.error(f"邀请用户时发生异常: {e}")
            return None, f"邀请用户时发生异常: {str(e)}"

    def add_user_to_group(self, user_id, group_id):
        """将用户添加到组"""
        if not user_id or not group_id:
            return False, "用户ID或组ID为空"
            
        payload = {"@odata.id": f"{GRAPH_ENDPOINT}/directoryObjects/{user_id}"}
        
        try:
            response = self._make_request('POST', f"{GRAPH_ENDPOINT}/groups/{group_id}/members/$ref", json=payload)
            if response.status_code == 204:
                logger.info(f"成功将用户 {user_id} 添加到组 {group_id}")
                return True, "添加成功"
            elif response.status_code == 400 and "one or more added object references already exist" in response.text.lower():
                return True, "用户已是成员"
            else:
                logger.error(f"添加用户失败: {response.status_code} - {response.text}")
                return False, f"添加失败: {response.status_code}"
        except Exception as e:
            logger.error(f"添加用户到组时发生异常: {e}")
            return False, f"添加用户到组时发生异常: {str(e)}"
            
    def update_user(self, user_id, payload):
        """使用用户ID更新用户信息"""
        if not user_id or not payload:
            return False, "用户ID或更新数据为空"
            
        # 验证和清理payload
        clean_payload = {}
        for key, value in payload.items():
            if value and str(value).strip():
                clean_payload[key] = str(value).strip()
                
        if not clean_payload:
            return False, "没有有效的更新数据"
            
        try:
            response = self._make_request('PATCH', f"{GRAPH_ENDPOINT}/users/{user_id}", json=clean_payload)
            if response.status_code == 204:
                logger.info(f"成功更新用户 {user_id}")
                return True, "更新成功"
            else:
                logger.error(f"更新用户失败: {response.status_code} - {response.text}")
                return False, f"更新失败: {response.status_code}"
        except Exception as e:
            logger.error(f"更新用户时发生异常: {e}")
            return False, f"更新用户时发生异常: {str(e)}"
            
    def get_group_members(self, group_id):
        """获取组成员列表"""
        if not group_id:
            return None
            
        members = []
        url = f"{GRAPH_ENDPOINT}/groups/{group_id}/members?$select=id,displayName,userPrincipalName,mail"
        
        try:
            while url:
                response = self._make_request('GET', url)
                data = response.json()
                members.extend(data.get('value', []))
                url = data.get('@odata.nextLink')
                
            logger.info(f"成功获取组 {group_id} 的 {len(members)} 个成员")
            return members
        except Exception as e:
            logger.error(f"获取组成员时发生异常: {e}")
            return None

# ==============================================================================
# 独立日志窗口
# ==============================================================================
class LogWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("操作日志")
        self.geometry("800x600")
        
        log_container = ttk.Frame(self)
        log_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.log_text = tk.Text(log_container, wrap=tk.WORD, height=15, state='disabled')
        log_scrollbar = ttk.Scrollbar(log_container, orient=tk.VERTICAL, command=self.log_text.yview)
        self.log_text.config(yscrollcommand=log_scrollbar.set)
        
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.protocol("WM_DELETE_WINDOW", self.withdraw)
        self.withdraw()

    def log(self, message):
        try:
            self.log_text.config(state='normal')
            self.log_text.insert(tk.END, f"{message}\n")
            self.log_text.see(tk.END)
            self.log_text.config(state='disabled')
        except tk.TclError:
            pass

# ==============================================================================
# Tkinter 应用类 (前端 UI 与交互逻辑)
# ==============================================================================
class SyncApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Excel to M365 Group Sync Tool (V8 - 智能逻辑)")
        self.geometry("900x650")
        
        self.api = GraphAPI()
        self.df = None
        self.file_path = None
        self.groups = []
        self.column_mappings = {}
        
        self.log_queue = queue.Queue()
        self.log_window = LogWindow(self)

        self._create_widgets()
        self.log("应用已启动。")
        
        self.poll_log_queue()
        self.connect_api()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        connect_frame = ttk.LabelFrame(main_frame, text="步骤 1: 连接与日志", padding="10")
        connect_frame.pack(fill=tk.X, pady=5)
        self.connect_button = ttk.Button(connect_frame, text="连接到 Microsoft Graph", command=self.connect_api)
        self.connect_button.pack(side=tk.LEFT, padx=(0, 10))
        self.connect_status = ttk.Label(connect_frame, text="状态: 未连接", foreground="red")
        self.connect_status.pack(side=tk.LEFT, padx=10)
        ttk.Button(connect_frame, text="显示/隐藏日志", command=self.toggle_log_window).pack(side=tk.RIGHT)

        file_frame = ttk.LabelFrame(main_frame, text="步骤 2: 选择源文件", padding="10")
        file_frame.pack(fill=tk.X, pady=5)
        ttk.Button(file_frame, text="选择 Excel 文件...", command=self.select_file).pack(side=tk.LEFT, padx=(0, 10))
        self.file_label = ttk.Label(file_frame, text="尚未选择文件")
        self.file_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(file_frame, text="选择工作表:").pack(side=tk.LEFT, padx=(10, 5))
        self.sheet_var = tk.StringVar()
        self.sheet_dropdown = ttk.Combobox(file_frame, textvariable=self.sheet_var, state='readonly', width=30)
        self.sheet_dropdown.pack(side=tk.LEFT)
        self.sheet_dropdown.bind("<<ComboboxSelected>>", self.load_columns)

        group_frame = ttk.LabelFrame(main_frame, text="步骤 3: 选择目标组", padding="10")
        group_frame.pack(fill=tk.X, pady=5)
        ttk.Label(group_frame, text="目标组:").pack(side=tk.LEFT, padx=(0, 10))
        self.group_var = tk.StringVar()
        self.group_dropdown = ttk.Combobox(group_frame, textvariable=self.group_var, state='readonly', width=50)
        self.group_dropdown.pack(side=tk.LEFT, fill=tk.X, expand=True)

        self.mapping_frame = ttk.LabelFrame(main_frame, text="步骤 4: 映射 Excel 列到用户属性", padding="10")
        self.mapping_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        ttk.Label(self.mapping_frame, text="请先选择工作表以加载列...").pack()

        style = ttk.Style()
        style.theme_use('clam')   # clam 主题对背景色支持较好
        style.configure('Run.TButton',
                background='#0078D4',   # 常规背景色
                foreground='white',     # 字体颜色
                padding=6)              # 按钮内部填充
        style.map('Run.TButton',
          background=[('active','#005A9E'),  # 鼠标按下或悬停时的背景
                      ('disabled','#A6A6A6')],
          foreground=[('disabled','#666666')])

        action_frame = ttk.Frame(main_frame)
        action_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=10, pady=10, ipady=10)
        action_frame.pack_propagate(False)
        action_frame.configure(height=60)
        self.run_button = ttk.Button(action_frame, 
                                     text="开始处理数据", 
                                     style='Run.TButton',
                                     command=self.start_processing_thread, 
                                     state=tk.DISABLED)
        self.run_button.pack()

    def log(self, message):
        try:
            if self.log_window and self.log_window.winfo_exists():
                self.log_window.log(message)
        except tk.TclError:
            # 日志窗口不可用，忽略错误
            pass
        
    def log_threadsafe(self, message):
        self.log_queue.put(message)

    def poll_log_queue(self):
        try:
            while True:
                message = self.log_queue.get_nowait()
                self.log(message)
        except queue.Empty: pass
        self.after(100, self.poll_log_queue)

    def toggle_log_window(self):
        if self.log_window.state() == "normal":
            self.log_window.withdraw()
        else:
            self.log_window.deiconify()

    def connect_api(self):
        self.log("正在尝试连接到 Microsoft Graph API...")
        self.connect_button.config(state=tk.DISABLED)
        threading.Thread(target=self._connect_api_worker, daemon=True).start()
        
    def _connect_api_worker(self):
        if self.api.get_access_token():
            self.log_threadsafe("成功连接到 Microsoft Graph API！")
            self.load_groups()
        else:
            self.log_threadsafe("错误: 连接失败。请检查凭据和网络。")
            self.after(0, self._update_ui_status, "连接失败", "red", tk.NORMAL)

    def _update_ui_status(self, status, color, button_state):
        try:
            if self.winfo_exists():
                self.connect_status.config(text=f"状态: {status}", foreground=color)
                self.connect_button.config(state=button_state)
        except (tk.TclError, AttributeError):
            pass

    def select_file(self):
        path = filedialog.askopenfilename(title="请选择 Excel 文件", filetypes=[("Excel Files", "*.xlsx *.xls")])
        if not path: return
        self.file_path = path
        self.file_label.config(text=path.split('/')[-1])
        self.log(f"已选择文件: {self.file_path}")
        try:
            xls = pd.ExcelFile(self.file_path)
            sheet_names = xls.sheet_names
            self.sheet_dropdown['values'] = sheet_names
            if sheet_names:
                self.sheet_var.set(sheet_names[0])
                self.load_columns()
        except Exception as e:
            messagebox.showerror("错误", f"无法读取 Excel 文件: {e}")

    def load_groups(self):
        self.log_threadsafe("正在加载组列表...")
        groups = self.api.get_all_groups()
        if groups is not None:
            self.groups = groups
            group_names = sorted([g['displayName'] for g in groups if g.get('displayName')])
            self.after(0, self._update_group_dropdown, group_names)
            self.log_threadsafe(f"成功加载 {len(group_names)} 个组。")
            self.after(0, self._update_ui_status, "已连接", "green", tk.NORMAL)
        else:
            self.log_threadsafe("加载组失败或未找到任何组。")
            self.after(0, self._update_ui_status, "加载组失败", "red", tk.NORMAL)

    def _update_group_dropdown(self, group_names):
        try:
            if self.winfo_exists() and hasattr(self, 'group_dropdown'):
                self.group_dropdown['values'] = group_names
                if group_names:
                    self.group_var.set(group_names[0])
        except (tk.TclError, AttributeError):
            pass

    def load_columns(self, *args):
        if not self.file_path or not self.sheet_var.get(): return
        try:
            self.df = pd.read_excel(self.file_path, sheet_name=self.sheet_var.get(), dtype=str).fillna('')
            columns = list(self.df.columns)
            self.log(f"已加载工作表 '{self.sheet_var.get()}'，包含 {len(self.df)} 行数据。")
            self._setup_mapping_ui(columns)
        except Exception as e:
            messagebox.showerror("错误", f"无法加载工作表 '{self.sheet_var.get()}': {e}")

    def _setup_mapping_ui(self, columns):
        for widget in self.mapping_frame.winfo_children():
            widget.destroy()
        self.column_mappings = {}
        azure_ad_fields = {
            "userPrincipalName": "用户主名称 (UPN) *必填*", "givenName": "名 (First Name)",
            "surname": "姓 (Last Name)", "displayName": "显示名称 (Display Name)",
            "companyName": "工作单位 (Company)", "department": "部门 (Department)",
            "jobTitle": "职务 (Job Title)", "mobilePhone": "电话 (Mobile Phone)"
        }
        for i, (field_key, field_label) in enumerate(azure_ad_fields.items()):
            ttk.Label(self.mapping_frame, text=field_label).grid(row=i, column=0, padx=5, pady=5, sticky="w")
            var = tk.StringVar()
            smart_choice = self._smart_match_column(field_key, columns)
            options = ["---忽略此字段---"] + columns
            dropdown = ttk.Combobox(self.mapping_frame, textvariable=var, values=options, state='readonly')
            var.set(smart_choice)
            dropdown.grid(row=i, column=1, padx=5, pady=5, sticky="ew")
            self.column_mappings[field_key] = var
        self.mapping_frame.columnconfigure(1, weight=1)
        self.run_button.config(state=tk.NORMAL)

    def _smart_match_column(self, field_key, columns):
        match_keywords = {
            "userPrincipalName": ["email", "邮箱", "邮件", "upn", "用户名", "account"],
            "givenName": ["first", "名", "given"], "surname": ["last", "姓", "family"],
            "displayName": ["display", "显示", "姓名", "name", "全名"],
            "companyName": ["company", "公司", "单位", "企业"],
            "department": ["department", "部门", "dept"],
            "jobTitle": ["title", "职位", "职务", "job"], "mobilePhone": ["phone", "电话", "手机", "mobile"]
        }
        keywords = match_keywords.get(field_key, [])
        for col in columns:
            col_lower = col.lower()
            for keyword in keywords:
                if keyword in col_lower:
                    return col
        return "---忽略此字段---"

    def start_processing_thread(self):
        if self.df is None:
            messagebox.showerror("错误", "请先选择并加载Excel文件。"); return
        if not self.group_var.get():
            messagebox.showerror("错误", "请选择一个目标组。"); return
        
        try:
            mappings = {key: var.get() for key, var in self.column_mappings.items()}
            target_group_name = self.group_var.get()
            df_copy = self.df.copy()
        except Exception as e:
            messagebox.showerror("错误", f"启动处理前发生错误: {e}"); return

        self.run_button.config(state=tk.DISABLED)
        threading.Thread(target=self.process_data, args=(mappings, target_group_name, df_copy), daemon=True).start()

    def process_data(self, mappings, target_group_name, df):
        try:
            self.log_threadsafe("="*50); self.log_threadsafe("开始预处理数据...")
            upn_col = mappings.get("userPrincipalName")
            if not upn_col or upn_col == "---忽略此字段---":
                self.log_threadsafe("致命错误: 必须映射“用户主名称 (UPN)”字段！"); return
            
            self.log_threadsafe(f"正在获取组 '{target_group_name}' 的 ID...")
            group_id = next((g['id'] for g in self.groups if g['displayName'] == target_group_name), None)
            if not group_id:
                self.log_threadsafe(f"致命错误: 找不到组 '{target_group_name}' 的 ID。"); return
            self.log_threadsafe(f"组 ID: {group_id}")

            # V8 核心改进: 预先获取所有组成员
            self.log_threadsafe("正在获取当前组成员列表以提高效率...")
            group_members = self.api.get_group_members(group_id)
            group_member_ids = {member['id'] for member in group_members} if group_members is not None else set()
            self.log_threadsafe(f"获取到 {len(group_member_ids)} 个现有成员。")
            
            tasks = []
            for index, row in df.iterrows():
                upn = str(row.get(upn_col, '')).strip().lower()
                if not upn:
                    self.log_threadsafe(f"警告: 第 {index + 2} 行的 UPN 为空，已跳过。")
                    continue
                    
                # 验证邮件格式
                if '@' not in upn or len(upn) < 5:
                    self.log_threadsafe(f"警告: 第 {index + 2} 行的 UPN '{upn}' 格式无效，已跳过。")
                    continue
                    
                display_name_col = mappings.get("displayName", "---忽略此字段---")
                if display_name_col != "---忽略此字段---":
                    display_name = str(row.get(display_name_col, '')).strip()
                else:
                    display_name = upn.split('@')[0]
                    
                if not display_name:
                    display_name = upn.split('@')[0]
                    
                tasks.append({
                    "line": index + 2, 
                    "upn": upn, 
                    "display_name": display_name, 
                    "excel_data": row, 
                    "action": "待定"
                })
            
            if not tasks:
                self.log_threadsafe("没有找到有效的数据行进行处理。"); return
            
            self.after(0, self.launch_confirmation_window, tasks, mappings, group_id, target_group_name, group_member_ids)
        finally:
            try:
                self.after(0, lambda: self.run_button.config(state=tk.NORMAL) if hasattr(self, 'run_button') else None)
            except (tk.TclError, AttributeError):
                pass

    def launch_confirmation_window(self, tasks, mappings, group_id, target_group_name, group_member_ids):
        try:
            confirmation_window = ConfirmationWindow(self, tasks, mappings, self.api, group_member_ids)
            self.wait_window(confirmation_window)
            final_tasks = getattr(confirmation_window, 'final_tasks', [])
            if not final_tasks:
                self.log("用户取消了操作或没有要执行的任务。"); return
            
            execution_thread = threading.Thread(target=self.execute_final_tasks, args=(final_tasks, group_id, target_group_name), daemon=True)
            execution_thread.start()
        except Exception as e:
            logger.error(f"启动确认窗口时发生异常: {e}")
            self.log(f"启动确认窗口时发生异常: {str(e)}")

    def execute_final_tasks(self, final_tasks, group_id, target_group_name):
        try:
            self.log_threadsafe("="*50); self.log_threadsafe("开始执行已确认的操作...")
            for task in final_tasks:
                action, upn, line, user_id = task.get("action"), task.get("upn"), task.get("line"), task.get("user_id")
                
                if action == 'update_only':
                    self.log_threadsafe(f"第 {line} 行 ({upn}): 正在仅更新用户信息...")
                    update_payload = task.get("update_payload", {})
                    if update_payload and user_id:
                        success, msg = self.api.update_user(user_id, update_payload)
                        self.log_threadsafe(f"  -> {'更新成功' if success else f'更新失败: {msg}'}")
                    else:
                        self.log_threadsafe(f"  -> 没有需要更新的信息或用户ID未知。")
                    continue

                if action == 'create':
                    self.log_threadsafe(f"第 {line} 行 ({upn}): 正在邀请新用户...")
                    new_user, msg = self.api.invite_guest_user(upn, task.get("display_name"))
                    if new_user:
                        user_id = new_user['id']; self.log_threadsafe(f"  -> 邀请成功。用户 ID: {user_id}")
                        task['user_id'] = user_id
                    else:
                        self.log_threadsafe(f"  -> 邀请失败: {msg}"); continue
                
                if action == 'update':
                    self.log_threadsafe(f"第 {line} 行 ({upn}): 正在更新用户信息...")
                    update_payload = task.get("update_payload", {})
                    if update_payload and user_id:
                        success, msg = self.api.update_user(user_id, update_payload)
                        self.log_threadsafe(f"  -> {'更新成功' if success else f'更新失败: {msg}'}")
                    else:
                        self.log_threadsafe(f"  -> 没有需要更新的信息或用户ID未知。")
                
                if user_id:
                    self.log_threadsafe(f"  -> 正在将用户添加到组 '{target_group_name}'...")
                    success, msg = self.api.add_user_to_group(user_id, group_id)
                    self.log_threadsafe(f"  -> {msg}")
                else:
                    self.log_threadsafe(f"  -> 警告: 无法添加到组，因为用户 ID 未知。")
            
            self.log_threadsafe("="*50); self.log_threadsafe("所有操作已完成！")
            self.show_final_group_info(group_id, target_group_name)
        except Exception as e:
            logger.error(f"执行任务时发生致命异常: {e}")
            self.log_threadsafe(f"执行任务时发生致命异常: {str(e)}")

    def show_final_group_info(self, group_id, group_name):
        self.log_threadsafe(f"正在获取组 '{group_name}' 的最新成员列表...")
        members = self.api.get_group_members(group_id)
        if members is not None:
            self.log_threadsafe(f"组 '{group_name}' 当前有 {len(members)} 个成员:")
            for i, member in enumerate(members[:20], 1):
                display_name = member.get('displayName', '未知')
                upn = member.get('userPrincipalName', member.get('mail', '未知'))
                self.log_threadsafe(f"  {i}. {display_name} ({upn})")
            if len(members) > 20: self.log_threadsafe(f"  ... 还有 {len(members) - 20} 个成员")
        else:
            self.log_threadsafe(f"无法获取组 '{group_name}' 的成员列表。")

# ==============================================================================
# 交互式确认与详情对比窗口
# ==============================================================================
class ConfirmationWindow(tk.Toplevel):
    def __init__(self, parent, tasks, mappings, api, group_member_ids):
        super().__init__(parent)
        self.title("操作确认 (双击行可查看/修改详情)")
        self.geometry("1200x700")
        self.transient(parent); self.grab_set()
        self.tasks = tasks; self.mappings = mappings; self.api = api; self.final_tasks = []
        self.group_member_ids = group_member_ids # V8 新增
        self._create_widgets()
        self.process_initial_check()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding="10"); main_frame.pack(fill=tk.BOTH, expand=True)
        info_label = ttk.Label(main_frame, text="正在检查用户状态... (双击行可查看详情和修改操作)")
        info_label.pack(pady=(0, 10))
        tree_frame = ttk.Frame(main_frame); tree_frame.pack(fill=tk.BOTH, expand=True)
        self.tree = ttk.Treeview(tree_frame, columns=("Line", "UPN", "Status", "Action"), show="headings")
        self.tree.heading("Line", text="行号"); self.tree.heading("UPN", text="用户主名称 (UPN)")
        self.tree.heading("Status", text="检查状态"); self.tree.heading("Action", text="待执行操作")
        self.tree.column("Line", width=80, anchor='center'); self.tree.column("UPN", width=300)
        self.tree.column("Status", width=250); self.tree.column("Action", width=200)
        v_scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        self.tree.grid(row=0, column=0, sticky='nsew'); v_scrollbar.grid(row=0, column=1, sticky='ns'); h_scrollbar.grid(row=1, column=0, sticky='ew')
        tree_frame.grid_rowconfigure(0, weight=1); tree_frame.grid_columnconfigure(0, weight=1)
        self.tree.bind("<Double-1>", self.show_detail_view)
        button_frame = ttk.Frame(main_frame); button_frame.pack(fill=tk.X, pady=(20, 0))
        ttk.Button(button_frame, text="全选执行", command=self.select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="全部跳过", command=self.deselect_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="执行已确认的操作", command=self.confirm_and_close).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="取消所有操作", command=self.cancel_and_close).pack(side=tk.RIGHT)

    def process_initial_check(self):
        threading.Thread(target=self._check_all_users, daemon=True).start()

    def _check_all_users(self):
        for task in self.tasks:
            item_id = str(task['line'])
            self.after(0, self._safe_tree_insert, item_id, task)
            user_info, msg = self.api.check_user_exists(task['upn'])
            task['user_info'] = user_info
            
            if user_info:
                task['user_id'] = user_info['id']
                diffs, payload = self._compare_data(task)
                task['update_payload'] = payload; task['data_diffs'] = diffs
                
                # V8 核心逻辑: 判断成员身份并设置智能默认操作
                is_in_group = user_info['id'] in self.group_member_ids
                
                if is_in_group:
                    if diffs:
                        status, action = "用户已在组内，数据有差异", "update_only"
                    else:
                        status, action = "用户已在组内，数据一致", "skip"
                else: # 不在组内
                    if diffs:
                        status, action = "用户存在，数据有差异", "update"
                    else:
                        status, action = "用户存在，数据一致", "add_only"
                task['action'] = action
            else:
                task['action'], task['user_id'] = "create", None
                status = msg
            
            self.after(0, self._safe_tree_update, item_id, status, self.get_action_display_name(task['action']))

    def _safe_tree_insert(self, item_id, task):
        try:
            if self.winfo_exists() and hasattr(self, 'tree'):
                self.tree.insert("", "end", values=(task['line'], task['upn'], "正在检查...", ""), iid=item_id)
        except (tk.TclError, AttributeError):
            pass

    def _safe_tree_update(self, item_id, status, action_display):
        try:
            if self.winfo_exists() and hasattr(self, 'tree'):
                current_values = self.tree.item(item_id, "values")
                if current_values:  # 确保项目存在
                    self.tree.item(item_id, values=(current_values[0], current_values[1], status, action_display))
        except (tk.TclError, AttributeError, IndexError):
            pass

    def _compare_data(self, task):
        diffs, payload = [], {}
        for key, col_name in self.mappings.items():
            if col_name != "---忽略此字段---" and key != "userPrincipalName":
                excel_val = str(task['excel_data'].get(col_name, '')).strip()
                azure_val = str(task['user_info'].get(key, '')).strip()
                if excel_val and excel_val != azure_val:
                    diffs.append(key); payload[key] = excel_val
        return diffs, payload

    def show_detail_view(self, event):
        item_id = self.tree.focus()
        if not item_id: return
        task_to_edit = next((t for t in self.tasks if t['line'] == int(item_id)), None)
        if not task_to_edit: return
        
        if task_to_edit.get('user_info'):
            detail_window = DetailComparisonWindow(self, task_to_edit, self.mappings)
            self.wait_window(detail_window)
            if hasattr(detail_window, 'result') and detail_window.result:
                task_to_edit['action'] = detail_window.result
                self._safe_tree_update(item_id, self.tree.item(item_id, "values")[2], self.get_action_display_name(detail_window.result))
        else:
            self.show_new_user_options(task_to_edit, item_id)

    def show_new_user_options(self, task, item_id):
        choice = messagebox.askyesnocancel("新用户操作", f"用户 {task['upn']} 不存在。\n\n点击 '是' 创建用户并添加到组\n点击 '否' 跳过此用户")
        if choice is not None:
            action = 'create' if choice else 'skip'
            task['action'] = action
            self._safe_tree_update(item_id, self.tree.item(item_id, "values")[2], self.get_action_display_name(action))

    def get_action_display_name(self, action_key):
        return {
            "create": "创建并添加到组", "update": "更新并添加到组",
            "add_only": "仅添加到组", "skip": "跳过此用户",
            "update_only": "仅更新用户数据"
        }.get(action_key, "未知")

    def select_all(self):
        for item_id in self.tree.get_children():
            task = next((t for t in self.tasks if t['line'] == int(item_id)), None)
            if task and task['action'] == 'skip':
                # 恢复到智能推荐的默认操作
                is_in_group = task['user_id'] in self.group_member_ids
                diffs = task.get('data_diffs')
                if is_in_group:
                    action = 'update_only' if diffs else 'skip' # 恢复后依然是skip
                else:
                    action = 'update' if diffs else 'add_only' if task.get('user_info') else 'create'
                task['action'] = action
                self._safe_tree_update(item_id, self.tree.item(item_id, "values")[2], self.get_action_display_name(action))

    def deselect_all(self):
        for item_id in self.tree.get_children():
            task = next((t for t in self.tasks if t['line'] == int(item_id)), None)
            if task:
                task['action'] = 'skip'
                self._safe_tree_update(item_id, self.tree.item(item_id, "values")[2], self.get_action_display_name('skip'))

    def confirm_and_close(self):
        self.final_tasks = [task for task in self.tasks if task['action'] != 'skip']
        if not self.final_tasks:
            if messagebox.askyesno("确认", "所有操作都已被设置为'跳过'，确定要关闭吗？"):
                self.destroy()
            return
        
        summary = {}
        for task in self.final_tasks:
            action_name = self.get_action_display_name(task['action'])
            summary[action_name] = summary.get(action_name, 0) + 1
        summary_text = "将要执行的操作:\n\n" + "\n".join([f"- {name}: {count} 个用户" for name, count in summary.items()])
        
        if messagebox.askyesno("最终确认", summary_text + "\n\n确定要执行这些操作吗？"):
            self.destroy()
        else:
            self.final_tasks = []

    def cancel_and_close(self):
        self.final_tasks = []; self.destroy()

# ==============================================================================
# 详细比较窗口
# ==============================================================================
class DetailComparisonWindow(tk.Toplevel):
    def __init__(self, parent, task, mappings):
        super().__init__(parent)
        self.title(f"详情对比 - {task['upn']}")
        self.geometry("800x600")
        self.transient(parent); self.grab_set()
        self.task = task; self.mappings = mappings; self.result = None
        
        self.style = ttk.Style(self)
        self.style.configure("Highlight.TLabel", foreground="#0078D4", font=("Helvetica", 11, "bold"))

        self._create_widgets()

    def _create_widgets(self):
        main_frame = ttk.Frame(self, padding="15"); main_frame.pack(fill=tk.BOTH, expand=True)
        ttk.Label(main_frame, text="数据对比", font=("Helvetica", 14, "bold")).pack(pady=(0, 20))
        comparison_frame = ttk.Frame(main_frame); comparison_frame.pack(fill=tk.BOTH, expand=True)
        ttk.Label(comparison_frame, text="属性", font=("Helvetica", 10, "bold")).grid(row=0, column=0, sticky='w', padx=5, pady=5)
        ttk.Label(comparison_frame, text="Azure AD 中的数据", font=("Helvetica", 10, "bold")).grid(row=0, column=1, sticky='w', padx=5, pady=5)
        ttk.Label(comparison_frame, text="Excel 中的新数据", font=("Helvetica", 10, "bold")).grid(row=0, column=2, sticky='w', padx=5, pady=5)
        ttk.Separator(comparison_frame, orient='horizontal').grid(row=1, column=0, columnspan=3, sticky='ew', pady=5)
        
        field_labels = {"userPrincipalName": "用户主名称", "givenName": "名", "surname": "姓", "displayName": "显示名称", "companyName": "工作单位", "department": "部门", "jobTitle": "职务", "mobilePhone": "电话"}
        row_idx = 2
        has_differences = False
        
        for key, label in field_labels.items():
            col_name = self.mappings.get(key)
            if not col_name or col_name == "---忽略此字段---": continue

            excel_val = str(self.task['excel_data'].get(col_name, '')).strip()
            azure_val = str(self.task['user_info'].get(key, '')).strip()
            
            is_different = excel_val and excel_val != azure_val

            if key == "userPrincipalName" and self.task['user_info'].get('userType') == 'Guest':
                formatted_excel_upn = excel_val.replace('@', '_').lower()
                if formatted_excel_upn in azure_val.lower():
                    is_different = False

            ttk.Label(comparison_frame, text=f"{label}:").grid(row=row_idx, column=0, sticky='w', padx=5, pady=2)
            ttk.Label(comparison_frame, text=azure_val if azure_val else "(空)").grid(row=row_idx, column=1, sticky='w', padx=5, pady=2)
            lbl_excel = ttk.Label(comparison_frame, text=excel_val if excel_val else "(空)")
            lbl_excel.grid(row=row_idx, column=2, sticky='w', padx=5, pady=2)
            
            if is_different:
                try:
                    lbl_excel.config(style="Highlight.TLabel")
                    has_differences = True
                except tk.TclError:
                    # 如果样式设置失败，忽略错误
                    has_differences = True
            row_idx += 1
        
        if not has_differences:
            ttk.Label(comparison_frame, text="数据完全一致，无需更新。", foreground="green", font=("Helvetica", 10, "italic")).grid(row=row_idx, column=0, columnspan=3, pady=10)
        
        button_frame = ttk.Frame(main_frame); button_frame.pack(fill=tk.X, pady=(20, 0))
        
        if has_differences:
            ttk.Button(button_frame, text="更新并添加到组", command=lambda: self.set_result_and_close('update')).pack(side=tk.LEFT, padx=5)
            ttk.Button(button_frame, text="仅更新用户数据", command=lambda: self.set_result_and_close('update_only')).pack(side=tk.LEFT, padx=5)

        ttk.Button(button_frame, text="仅添加到组 (不更新)", command=lambda: self.set_result_and_close('add_only')).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="跳过此用户", command=lambda: self.set_result_and_close('skip')).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="取消", command=self.destroy).pack(side=tk.RIGHT, padx=5)

    def set_result_and_close(self, action):
        self.result = action; self.destroy()

# ==============================================================================
# 主程序入口
# ==============================================================================
if __name__ == "__main__":
    try:
        app = SyncApp()
        app.mainloop()
    except EnvironmentError as e:
        logger.error(f"环境配置错误: {e}")
        messagebox.showerror("配置错误", str(e))
    except Exception as e:
        logger.error(f"应用启动时发生致命错误: {e}")
        messagebox.showerror("启动错误", f"应用启动时发生致命错误: {str(e)}")
