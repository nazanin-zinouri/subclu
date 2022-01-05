# Purpose

We might be using different notebooks, so keep configs in this file so that it's easy to share them across VMs/notebooks.

To update/access these settings, in the jupyterlab UI click on:
- `Settings` > `Advanced Settings Editor` 
  - Keyboard shortcut: `⌘ ,` 

Then you can click on one of the settings tabs to update your `User Preferences`.

More info: https://jupyter-notebook.readthedocs.io/en/stable/extending/keymaps.html

# Theme
By default, jupyter's theme is Light (white background). We can change it to dark mode under:<br>
`Settings > Theme`

```
{
    // Selected Theme
    // Application-level visual styling theme
    "theme": "JupyterLab Dark",
}
```

# Keyboard shortcuts
User preferences:
```
{
    
    "shortcuts": [
    // Move cell up
        {
            "selector": ".jp-Notebook:focus",
            "command": "notebook:move-cell-up",
            "keys": [
                "Alt ArrowUp"
            ]
        },
    // Move cell down
        {
            "selector": ".jp-Notebook:focus",
            "command": "notebook:move-cell-down",
            "keys": [
                "Alt ArrowDown"
            ]
        },
        {
            "selector": ".jp-Notebook.jp-mod-editMode",
            "command": "notebook:run-all-above",
            "keys": [
                "Shift Accel Enter"
            ],
        },
    ]
}
```

# Notebook
```
{
    "codeCellConfig": {
        "codeFolding": true,
        "autoClosingBrackets": true,
    }
}
```

# Status bar
This thing can be annoying because its flashing and changing all the time
```
{
    // Status Bar
    // @jupyterlab/statusbar-extension:plugin
    // Status Bar settings.
    // **************************************

    // Status Bar Visibility
    // Whether to show status bar or not
    "visible": false
}
```

# Text editor
I don't use it much, but definitely want to have code folding enabled
```
{
    "editorConfig": {
        "codeFolding": true
    }
}
```
