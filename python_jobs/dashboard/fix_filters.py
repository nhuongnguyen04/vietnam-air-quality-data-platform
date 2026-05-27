import os
import glob

# Fix lib/filters.py indentation and names
with open('lib/filters.py', 'r') as f:
    lines = f.readlines()

new_lines = []
indent_level = 4
inside_popover = False

for line in lines:
    if line.strip().startswith('with c') and ':' in line:
        new_lines.append(line)
        continue
    if line.strip().startswith('with st.popover'):
        new_lines.append(line)
        inside_popover = True
        continue
    if line.strip().startswith('return {'):
        inside_popover = False
        new_lines.append(line)
        continue
    
    if inside_popover and line.startswith('    ') and not line.startswith('        ') and line.strip() != '':
        # It has 4 spaces but needs 8 spaces
        new_lines.append('    ' + line)
    else:
        new_lines.append(line)

with open('lib/filters.py', 'w') as f:
    f.writelines(new_lines)

# Rename render_sidebar_filters to render_top_filters in all pages
for filepath in glob.glob('pages/*.py'):
    with open(filepath, 'r') as f:
        content = f.read()
    
    content = content.replace('render_sidebar_filters', 'render_top_filters')
    
    with open(filepath, 'w') as f:
        f.write(content)

print("Fix completed")
