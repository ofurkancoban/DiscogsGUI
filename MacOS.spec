# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    # assets klasörünü dosya olarak ekliyoruz
    datas=[('assets', 'assets')],
    # Tkinter ve ttkbootstrap gibi modülleri hiddenimports içine ekliyoruz.
    hiddenimports=['tkinter', 'ttkbootstrap'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    # Gerekirse konsol çıktısını görmek için console=True yapabilirsiniz.
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,

    entitlements_file=None,
    icon=['assets/app_icon.png'],
)
app = BUNDLE(
    exe,
    name='Discogs Data Processor.app',
    icon='assets/app_icon.ico',
    bundle_identifier='com.ofurkancoban.DDP',
    info_plist={
        'LSUIElement': False,  # Uygulamanın Dock'ta görünmesini sağlar
        'NSHighResolutionCapable': True
    }
)