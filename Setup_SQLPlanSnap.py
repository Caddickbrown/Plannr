from setuptools import setup
import py2exe

setup(
    options={
        'py2exe': {
            'compressed': True,
            'includes': [
                # Database connectivity
                'sqlalchemy.dialects.mssql.pyodbc',
                'pyodbc',
                'pymssql',
                
                # Azure authentication
                'azure.identity',
                'msal',
                'requests',
                
                # Data processing
                'pandas',
                'openpyxl',
                'openpyxl.styles',
                'openpyxl.utils',
                
                # Environment and system
                'dotenv',
                'psutil',
                
                # GUI components
                'tkinter',
                'tkinter.filedialog',
                'tkinter.messagebox',
                'tkinter.ttk',
                
                # Standard library modules that might be missed
                'json',
                'threading',
                'io',
                'typing',
                'datetime',
                'gc'
            ]
        }
    },
    windows=[{
        'script': 'AMCBDG_MFA.py',
        'icon_resources': [(1, 'Assets/PlanSnapBlue.ico')]
    }],
    data_files=[
        ('', ['db_credentials.env']),
        ('Assets', ['Assets/PlanSnapRed.ico'])
    ]
)
