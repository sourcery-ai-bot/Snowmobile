# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

# sys.path.insert(0, os.path.abspath('.'))
print(os.path.abspath('source'))

# ----------/GEM/-----------------------
sys.path.insert(0, os.path.join(os.path.abspath('source').split('docs')[0],
                                'src', 'snowmobile'))

# print(f"GEM-os.path.abspath('.'):\n\t{os.path.abspath('source')}\n")
# print(f"GEM-os.path.abspath('..'):\n\t{os.path.abspath('')}\n")

# -- Project information -----------------------------------------------------

project = 'snowmobile'
copyright = '2020, Grant E Murray'
author = 'Grant E Murray'

# The full version, including alpha/beta/rc tags
import subprocess
release = \
    str((subprocess.check_output(['git', 'describe']).strip())). \
        replace("'", '').replace('b', '')

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ['sphinx.ext.autodoc', 'sphinx.ext.intersphinx',
              'sphinx.ext.todo',
              'sphinx.ext.autosummary', 'sphinx.ext.viewcode',
              'sphinx.ext.coverage',
              'sphinx.ext.doctest', 'sphinx.ext.ifconfig',
              'sphinx.ext.mathjax',
              'sphinx.ext.napoleon', 'sphinx_rtd_theme', 'recommonmark',
              'autoapi.extension']

autoapi_type = 'python'

autoapi_dirs = ['../src/snowmobile', '../AUTHORS.rst',
                '../LICENSE.txt',
                '../CHANGELOG.rst', '../README.md']

autoapi_ignore = ['main.py', '__init__.py']

autoapi_python_class_content = 'both'

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = 'python'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'main.py', '__init__.py']

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = True
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = False
napoleon_use_rtype = True
napoleon_use_keyword = True
napoleon_custom_sections = 'Attributes'

# Suffix of source filenames
source_suffix = ['.rst', '.md']

# Scaffolding paste from here down ============================================

# If not '', a 'Last updated on:' timestamp is inserted at every page bottom,
# using the given strftime format.
# html_last_updated_fmt = '%b %d, %Y'

# If true, SmartyPants will be used to convert quotes and dashes to
# typographically correct entities.
html_use_smartypants = True

# Custom sidebar templates, maps document names to template names.
# html_sidebars = {}

# Additional templates that should be rendered to pages, maps page names to
# template names.
# html_additional_pages = {}

# If false, no module index is generated.
# html_domain_indices = True

# If false, no index is generated.
# html_use_index = True

# If true, the index is split into individual pages for each letter.
# html_split_index = False

# If true, links to the reST sources are added to the pages.
html_show_sourcelink = True

# If true, "Created using Sphinx" is shown in the HTML footer. Default is True.
html_show_sphinx = False

# If true, "(C) Copyright ..." is shown in the HTML footer. Default is True.
html_show_copyright = False

# If true, an OpenSearch description file will be output, and all pages will
# contain a <link> tag referring to it.  The value of this option must be the
# base URL from which the finished HTML is served.
# html_use_opensearch = ''

# This is the file name suffix for HTML files (e.g. ".xhtml").
# html_file_suffix = None

# Output file base name for HTML help builder.
# htmlhelp_basename = 'cusersgem7318documentsgithubsnow_parser-doc'


# -- Options for LaTeX output --------------------------------------------------

# latex_elements = {
# # The paper size ('letterpaper' or 'a4paper').
# # 'papersize': 'letterpaper',
#
# # The font size ('10pt', '11pt' or '12pt').
# # 'pointsize': '10pt',
#
# # Additional stuff for the LaTeX preamble.
# # 'preamble': '',
# }

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title, author, documentclass [howto/manual]).
# latex_documents = [
#   ('index', 'user_guide.tex', u'C:\Users\GEM7318\Documents\Github\Snow-Parser Documentation',
#    u'Grant E Murray', 'manual'),
# ]

# The name of an image file (relative to this directory) to place at the top of
# the title page.
# latex_logo = ""

# For "manual" documents, if this is true, then toplevel headings are parts,
# not chapters.
# latex_use_parts = False

# If true, show page references after internal links.
latex_show_pagerefs = True

# Highlighting for code blocks
pygments_style = 'sphinx'

# If true, show URL addresses after external links.
# latex_show_urls = False

# Documents to append as an appendix to all manuals.
# latex_appendices = []

# If false, no module index is generated.
# latex_domain_indices = True

# -- External mapping ------------------------------------------------------------
python_version = '.'.join(map(str, sys.version_info[0:2]))
intersphinx_mapping = {
    'sphinx': ('http://www.sphinx-doc.org/en/stable', None),
    'python': ('https://docs.python.org/' + python_version, None),
    'matplotlib': ('https://matplotlib.org', None),
    'numpy': ('https://docs.scipy.org/doc/numpy', None),
    'sklearn': ('http://scikit-learn.org/stable', None),
    'pandas': ('http://pandas.pydata.org/pandas-docs/stable', None),
    'scipy': ('https://docs.scipy.org/doc/scipy/reference', None),
}

# Adding so that __init__ will be documented - source is from link below:
# https://stackoverflow.com/questions/5599254/how-to-use-sphinxs-autodoc-to-document-a-classs-init-self-method

# def skip(app, what, name, obj, would_skip, options):
#     if name == "__init__":
#         return False
#     return would_skip
#
# def setup(app):
#     app.connect("autodoc-skip-member", skip)

autodoc_default_options = {
    'members': 'var1, var2',
    # 'member-order': 'bysource',
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__',
    'ignore-module-all': ['main.py'],
    'autoclass_content': 'both'
}


# To configure AutoStructify
def setup(app):
    from recommonmark.transform import AutoStructify
    app.add_config_value('recommonmark_config', {
        'auto_toc_tree_section': 'Contents',
        'enable_eval_rst': True,
        'enable_math': True,
        'enable_inline_math': True
    }, True)
    app.add_transform(AutoStructify)


# github_doc_root = 'https://github.com/GEM7318/Snowmobile/tree/master/docs'
# def setup(app):
#     from recommonmark.transform import AutoStructify
#     app.add_config_value('recommonmark_config', {
#             'url_resolver': lambda url: github_doc_root + url,
#             'auto_toc_tree_section': 'Contents',
#             }, True)
#     app.add_transform(AutoStructify)

# Turning off autoapi index since I manually added to TOC tree
autoapi_add_toctree_entry = False

# from sphinx.ext.autodoc import s
