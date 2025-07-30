from distutils.core import setup
import py2exe

setup(
    options={
        'py2exe': {
            'compressed': True,
            'includes': [
                'sqlalchemy.dialects.mssql.pyodbc',
                'pyodbc',
		'pymssql'
            ]
        }
    },
    windows=[{
        'script': 'AMCBDG_SQL.py',
        'icon_resources': [(1, 'Assets/PlanSnapBlue.ico')]
    }],
    data_files=[
        ('', ['db_credentials.env']),
        ('Assets', ['Assets/PlanSnapBlue.ico'])
    ]
)
