from distutils.core import setup
import py2exe

setup(
    options={'py2exe': {'compressed': True}},
    windows=[{
        'script': 'AMCBDG.py',
        'icon_resources': [(1, 'AMCBDG2.ico')]
    }]
)
