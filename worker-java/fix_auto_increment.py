import os
import re

src_dir = r"c:\Users\ruanp\NETBENAS - COWORK\migrador-web\worker-java\src\br\com\lcsistemas"

for root, dirs, files in os.walk(src_dir):
    for file in files:
        if file.endswith(".java"):
            filepath = os.path.join(root, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # replace lines like: execIgnore(my, "ALTER TABLE lc_sistemas.ncm AUTO_INCREMENT = 1", "reset AI ncm");
            # by commenting them out. Some are split across lines, but wait, the grep output showed them.
            # Let's just match any line that contains AUTO_INCREMENT = 1 and if it doesn't start with //, prepend //
            
            lines = content.split('\n')
            modified = False
            for i, line in enumerate(lines):
                if 'AUTO_INCREMENT = 1' in line and not line.strip().startswith('//'):
                    lines[i] = '// ' + line
                    modified = True
            
            if modified:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))
                print("Modified", filepath)
