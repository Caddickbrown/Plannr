from distutils.core import setup
import py2exe

setup(
    options={'py2exe': {'compressed': True}},
    windows=[{
        'script': 'AMCBDG_SQL.py',
        'icon_resources': [(1, 'Assets/PlanSnapBlue.ico')]
    }]
)
