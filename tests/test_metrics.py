import sys
import types
import os

# Provide minimal tkinter and other dependencies for AMCBD import
if 'tkinter' not in sys.modules:
    tk_mod = types.ModuleType('tkinter')

    class DummyTk:
        def title(self, *args, **kwargs):
            pass

        def geometry(self, *args, **kwargs):
            pass

        def mainloop(self):
            pass

    tk_mod.Tk = DummyTk
    tk_mod.END = 'end'
    tk_mod.Text = object
    sys.modules['tkinter'] = tk_mod
    sys.modules['tkinter.filedialog'] = types.ModuleType('tkinter.filedialog')
    sys.modules['tkinter.messagebox'] = types.ModuleType('tkinter.messagebox')
    sys.modules['tkinter.ttk'] = types.ModuleType('tkinter.ttk')

for mod in ['pandas', 'openpyxl']:
    if mod not in sys.modules:
        sys.modules[mod] = types.ModuleType(mod)

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, root_dir)

import ast

# Load only the helper functions from AMCBD without executing the GUI code
with open(os.path.join(root_dir, 'AMCBD.py'), 'r') as f:
    tree = ast.parse(f.read(), filename='AMCBD.py')

namespace = {}
for node in tree.body:
    if isinstance(node, ast.FunctionDef) and node.name in {'format_metric', 'safe_metric'}:
        module = ast.Module([node], type_ignores=[])
        exec(compile(module, 'AMCBD.py', 'exec'), namespace)

format_metric = namespace['format_metric']
safe_metric = namespace['safe_metric']


def test_format_metric_number():
    assert format_metric(1234, 'number') == '1,234'


def test_format_metric_hours():
    assert format_metric(10, 'hours') == '10.0'


def test_format_metric_percentage():
    assert format_metric(25.678, 'percentage') == '25.7%'


def test_format_metric_invalid():
    assert format_metric('bad', 'hours') == '0'


def test_safe_metric_defaults():
    metrics = {'a': 1}
    assert safe_metric(metrics, 'a') == 1
    assert safe_metric(metrics, 'b') == 0
    assert safe_metric(metrics, 'b', default=5) == 5

