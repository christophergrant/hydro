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
from __future__ import annotations

import os
import sys
from datetime import date

from packaging.version import parse

from hydro import __version__


def minor_major(version):
    v = parse(version)
    return f'{v.major}.{v.minor}'


sys.path.insert(0, os.path.abspath('../..'))
# -- Project information -----------------------------------------------------

project = 'hydro'
copyright = f'{date.today().year} Christopher Grant'
author = 'Christopher Grant'

# The full version, including alpha/beta/rc tags
release = minor_major(__version__)

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'myst_parser',
    'sphinx.ext.autodoc',
    'sphinx.ext.intersphinx',
    'sphinx.ext.viewcode',
    'sphinx_design',
    # "sphinxext.rediraffe",
    'sphinxcontrib.mermaid',
    'sphinxext.opengraph',
    'sphinx.ext.napoleon',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

intersphinx_mapping = {
    'pyspark': (
        'https://spark.apache.org/docs/latest/api/python/',
        None,
    ),
    'delta': ('https://docs.delta.io/latest/api/python/', None),
}

# -- Autodoc settings ---------------------------------------------------

nitpicky = True
autodoc_member_order = 'bysource'
nitpick_ignore = [
    ('py:class', 'docutils.nodes.document'),
    ('py:class', 'docutils.nodes.docinfo'),
    ('py:class', 'docutils.nodes.Element'),
    ('py:class', 'docutils.nodes.Node'),
    ('py:class', 'docutils.nodes.field_list'),
    ('py:class', 'docutils.nodes.problematic'),
    ('py:class', 'docutils.nodes.pending'),
    ('py:class', 'docutils.nodes.system_message'),
    ('py:class', 'docutils.statemachine.StringList'),
    ('py:class', 'docutils.parsers.rst.directives.misc.Include'),
    ('py:class', 'docutils.parsers.rst.Parser'),
    ('py:class', 'docutils.utils.Reporter'),
    ('py:class', 'nodes.Element'),
    ('py:class', 'nodes.Node'),
    ('py:class', 'nodes.system_message'),
    ('py:class', 'Directive'),
    ('py:class', 'Include'),
    ('py:class', 'StringList'),
    ('py:class', 'DocutilsRenderer'),
    ('py:class', 'MockStateMachine'),
]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_book_theme'
html_title = ''
html_theme_options = {
    'home_page_in_toc': True,
    'github_url': 'https://github.com/christophergrant/delta-hydro',
    'repository_url': 'https://github.com/christophergrant/delta-hydro',
    'repository_branch': 'main',
    'path_to_docs': 'docs/source',
    'use_repository_button': True,
    'use_edit_page_button': True,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


# -- Myst options -------------------------------------------------

myst_enable_extensions = [
    'dollarmath',
    'amsmath',
    'deflist',
    'fieldlist',
    'html_admonition',
    'html_image',
    'colon_fence',
    'smartquotes',
    'replacements',
    'linkify',
    'strikethrough',
    'substitution',
    'tasklist',
    #'attrs_inline',
]
myst_number_code_blocks = ["typescript"]
myst_heading_anchors = 2
myst_footnote_transition = True
myst_dmath_double_inline = True
