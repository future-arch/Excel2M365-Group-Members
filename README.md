# Excel to M365 Group Sync Tool

A secure Python application for synchronizing user data from Excel files to Microsoft 365 groups using Microsoft Graph API.

## Security Improvements Made

### 🔒 Critical Security Fixes
- **Removed hardcoded credentials** - Now uses environment variables
- **Added input validation** - Prevents injection attacks
- **Improved error handling** - No sensitive data in error messages
- **Added request timeout** - Prevents hanging connections
- **URL encoding** - Prevents URL injection attacks

### 🛡️ Additional Security Features
- Token caching with expiration
- Rate limiting handling
- Request retry mechanism
- File size validation (50MB limit)
- Email format validation
- Secure session management

## Setup Instructions

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your Azure AD credentials:
```bash
cp .env.example .env
```

Edit `.env` file with your actual values:
```
AZURE_CLIENT_ID=your-client-id-here
AZURE_CLIENT_SECRET=your-client-secret-here
AZURE_TENANT_ID=your-tenant-id-here
```

### 3. Azure AD App Registration
1. Go to Azure Portal → Azure Active Directory → App registrations
2. Create new application or use existing one
3. Note down Application (client) ID and Directory (tenant) ID
4. Create a client secret
5. Grant the following Microsoft Graph API permissions:
   - `User.ReadWrite.All`
   - `Group.ReadWrite.All`
   - `Directory.ReadWrite.All`

### 4. Run the Application
```bash
python src/email2members_copy.py
```

## Features

- ✅ Secure credential management
- ✅ Excel file validation and processing
- ✅ Microsoft Graph API integration
- ✅ User existence checking
- ✅ Guest user invitation
- ✅ Group membership management
- ✅ Data comparison and updates
- ✅ Interactive confirmation dialogs
- ✅ Comprehensive logging
- ✅ Error handling and recovery

## Usage

1. **Connect to Microsoft Graph** - Click "连接到 Microsoft Graph"
2. **Select Excel File** - Choose your user data file
3. **Map Columns** - Map Excel columns to user attributes
4. **Select Target Group** - Choose the M365 group
5. **Review and Confirm** - Review planned actions
6. **Execute** - Run the synchronization

## Security Best Practices

- Never commit `.env` file to version control
- Use least privilege principle for API permissions
- Regularly rotate client secrets
- Monitor application logs for suspicious activity
- Keep dependencies up to date

## Troubleshooting

### Common Issues
1. **Environment Variables Not Set**: Check `.env` file exists and has correct values
2. **API Connection Failed**: Verify Azure AD app permissions and credentials
3. **File Too Large**: Excel files must be under 50MB
4. **Invalid Email Format**: Ensure UPN column contains valid email addresses

### Logs
Check the application logs for detailed error information. The log window can be toggled using the "显示/隐藏日志" button.