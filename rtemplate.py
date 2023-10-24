#!/usr/bin/env python3

# Features:
#   DONE SQL blocks
#   DONE SQL separators (SEP)
#   DONE Inline escape
#   DONE Whitespace trim
#   DONE Macros
#   DONE SQL init and cleanup sections
#   TODO Argv & Env
#   DONE mcpp
#   DONE mcpp should not be default
#   DONE sys_Write table
#   DONE INSERT
#   DONE Hygienic aliases (`$` aliases).
#   TODO Keyword arguments for macros.
#   More escapes needed:
#     TODO {{{ expr }} something} --> {value something}
#     TODO Mcpp cannot handle double quoted strings starting on one line
#          and closing on another.
#   TODO Inherit indentation.
#   DONE Better defaults for whitespace trim.
#   DONE sys_Write paths must be relative paths. (TODO Write test driver...)
#   DONE sys_Write paths must use restricted set of characters. (TODO Write test driver...)
#   TODO Supply prefix to write sys_Write contents to.
#   DONE Automatically create directories for sys_Write paths. (TODO Write test driver...)
#   DONE Comments {# ... #}.
#   TODO Define extension functions for SQL.
#   TODO Add mode to run on db containing sys_Write table.
#   TODO Syntax for empty result sets {% QUERY... %}...{% ELSE %}...{% END %}
#   TODO Inject sql commands via CLI (e.g. as with `sqlite3 -cmd ...`)
#   TODO Propagate line number information from template to transpiled code ("sourcemap")
#   TODO Unit test group_concat() cases 0, 1, many

# Next milestone:
#   TODO Better error messages.
#   TODO Modules.

'''
Init, cleanup, and code sections
================================

::
  %% init   Init section: Will be executed before any code.
            Section is filled with SQL.
  %% fini   Fini section: Will be executed after code.
            Section is filled with SQL.
  %% code   Template code follows.
  %% done   Everything that follows is ignored until a new section is opened.

Code sections
=============

Code sections consist of plain text, copied directly to the output, mixed with
SQL blocks and inline escapes that allow text to be substituted.

SQL blocks
==========

::
  {% <SQL_CONTENT> %}
  {% END %}

SQL separators (SEP token)
==========================

::
  {% <SQL_CONTENT> SEP '<SEPARATOR>' %}
  {% END %}

Whitespace trim
===============

::
  {% ... %}       Trim nothing.
  {%- ... %}      Trim to beginning of current line.
  {%-- ... %}     Trim to beginning of current line and preceding newline.
  {%--- ... %}    Trim all preceding whitespace.
  {% ... -%}      Trim to end of current line.
  {% ... --%}     Trim to end of line and trim newline.
  {% ... ---%}    Trim all succeeding whitespace.

Inline escape
=============

::
  {{ <EXPRESSION> }}
  {{ call <MACRO_NAME>([expression...]) }}

`<EXPRESSION>` is one of:
- SQL expression.
- Template macro instantiation (`call <MACRO_NAME>(...)`).

Template macro
==============

::
  {% macro <NAME>(<ARG>...) %}
  {% endmacro %}

Mcpp
====

Escapes
=======

In top level::
  "^%% %% ..."      Generate a literal "%% ..."

In SQL blocks::
  "SEP '<QUOTED>'"  QUOTED escapes:
                      "''" becomes "'"
                      '\\n' becomes x'0a' (a newline)
                      '\\<anything else>' is an error
                      '\\\\' becomes '\\'
'''

import os
import re
import sys
import json
import tempfile
import simplejson
import argparse
import subprocess
import shutil
import sqlite3
from contextlib import contextmanager
from collections import namedtuple

g_applicationMagicString = 'reltpl'

class Options(namedtuple('Options', ['includePaths', 'db', 'sysDb', 'noMcpp', 'noRender', 'noSqlite3', 'quiet', 'source', 'prefix'])):

    @classmethod
    def fromArgv(cls, argv):
        parser = argparse.ArgumentParser()
        parser.add_argument('-q', dest='quiet', action='store_true')
        parser.add_argument('-I', metavar='DIR', dest='includePaths', type=str, action='append')
        parser.add_argument('--db', dest='db', type=str)
        parser.add_argument('--sys-db', dest='sysDb', type=str)
        parser.add_argument('--mcpp', dest='yesMcpp', action='store_true')
        parser.add_argument('--no-mcpp', dest='noMcpp', action='store_true')
        parser.add_argument('--no-render', dest='noRender', action='store_true')
        parser.add_argument('--no-sqlite3', dest='noSqlite3', action='store_true')
        parser.add_argument('--prefix', dest='prefix', type=str)
        parser.add_argument('source', metavar='SOURCE', type=str, nargs=1)
        args = parser.parse_args(args=argv)
        return Options(
            includePaths=args.includePaths or [],
            db=args.db,
            sysDb=args.sysDb,
            noMcpp=not args.yesMcpp,
            noRender=args.noRender,
            noSqlite3=args.noSqlite3,
            quiet=args.quiet,
            prefix=args.prefix,
            source=args.source[0],
        )

    def findExecutable(self, filename):
        if '/' in filename or '\\' in filename:
            found = os.path.abspath(os.path.expanduser(filename))
        else:
            searchPaths = os.environ['PATH'].split(os.pathsep)
            for path in searchPaths:
                maybe = os.path.join(path, filename)
                if os.path.exists(maybe):
                    found = maybe
                    break
        return found

class FileUtil:
    @classmethod
    @contextmanager
    def tempdir(cls):
        tmpdir = tempfile.mkdtemp(suffix='', prefix=g_applicationMagicString)
        try:
            yield tmpdir
        finally:
            try: # See http://stackoverflow.com/questions/6884991/how-to-delete-dir-created-by-python-tempfile-mkdtemp
                shutil.rmtree(tmpdir)
                pass
            except OSError as esc:
                if exc.errno != errno.ENOENT:
                    raise # re-raise exception

class Token(namedtuple('Token', ['kind', 'strKind', 'value', 'start', 'end'])):
    pass

class ParseError(RuntimeError):
    def __init__(self, expected, token):
        RuntimeError.__init__(self, 'ParseError: expected ' + expected + ', got ' + repr(token))

class TextUtil:

    d_sqlSplitRe = re.compile(r'''[(),.]|[^(),.'"\s]+|\s+|'(?:''|[^'])*'|"(?:""|[^"])*"''')

    @classmethod
    def sqlSplit(cls, x):
        return cls.d_sqlSplitRe.findall(x)

    @classmethod
    def sqliteQuote(cls, x):
        def quote_imp(s):
            return "'" + s.replace("'", "''") + "'"
        x.replace('\r\n', '\n')
        x.replace('\n\r', '\n')
        quoted = " || x'0a' || ".join(map(quote_imp, x.split('\n')))
        return quoted

    @classmethod
    def ltrim(cls, x, amount):
        if 0 == amount:
            return x
        if 1 == amount:
            return re.sub(r'^[ \t\f\v]*', '', x, count=1)
        if 2 == amount:
            return re.sub(r'^[ \t\f\v]*(\r\n|\n\r|\n|\r)?', '', x, count=1)
        if 3 == amount:
            return re.sub(r'^\s*', '', x, count=1)
        assert(not "Shouldn't be here.")

    @classmethod
    def rtrim(cls, x, amount):
        if 0 == amount:
            return x
        if 1 == amount:
            return re.sub(r'[ \t\f\v]*$', '', x, count=1)
        if 2 == amount:
            return re.sub(r'(\r\n|\n\r|\n|\r)?[ \t\f\v]*$', '', x, count=1)
        if 3 == amount:
            return re.sub(r'\s*$', '', x, count=1)
        assert(not "Shouldn't be here.")

class RenderContext:
    def __init__(self):
        self.d_level = 0
        self.d_lines = []
        self.d_innerLeftTrim = [3]
        self.d_innerRightTrim = [3]
        self.d_bindings = [{}]
        self.d_aliases = [{}]
        self.d_macros = {}

    def line(self, line):
        self.d_lines.append(line)

    def innerLeftTrim(self):
        return self.d_innerLeftTrim[-1]

    def innerRightTrim(self):
        return self.d_innerRightTrim[-1]

    def registerMacro(self, astMacro):
        self.d_macros[astMacro.name.value] = astMacro

    def invokeMacro(self, name, args):
        if name not in self.d_macros:
            print(self.d_macros.keys(), file=sys.stderr)
            raise RuntimeError('No macro found by name: ' + name)
        macro = self.d_macros[name]
        assert(len(args) == len(macro.bindVars))
        bindings = { }
        for arg, binding in zip(args, macro.bindVars):
            bindings[binding.value] = arg
        self.d_bindings.append(bindings)
        self.d_aliases.append({})

        macro.invoke_imp(self)

        self.d_bindings.pop()
        self.d_aliases.pop()

    def createAlias(self, hygienicAliasToken):
        alias = hygienicAliasToken.value
        assert(alias.startswith('$'))
        name = '_%d_%s' % (self.d_level, alias[1:])
        if alias in self.d_aliases[-1]:
            #raise RuntimeError("Alias already used: " + repr(hygienicAliasToken))
            pass
        else:
            self.d_aliases[-1][alias] = name
        return name

    def getBindValue(self, bindVarToken):
        var = bindVarToken.value
        assert(var.startswith('@'))
        return self.d_bindings[-1][var]

    def getAliasValue(self, hygienicAliasToken):
        alias = hygienicAliasToken.value
        assert(alias.startswith('$'))
        return self.d_aliases[-1][alias]

class AstTop(namedtuple('AstTop', ['sections'])):

    def render_imp(self, cx):
        inits = []
        finis = []
        codes = []
        for section in self.sections:
            if isinstance(section, AstInit):
                inits.append(section)
            elif isinstance(section, AstFini):
                finis.append(section)
            elif isinstance(section, AstBody):
                codes.append(section)

        for section in inits:
            section.render_imp(cx)

        for section in codes:
            section.render_imp(cx)

        for section in reversed(finis):
            section.render_imp(cx)

    def render(self):
        cx = RenderContext()
        self.render_imp(cx)
        return '\n'.join(cx.d_lines)

class AstInit(namedtuple('AstInit', ['text'])):

    def render_imp(self, cx):
        cx.line(self.text.value)

class AstFini(namedtuple('AstInit', ['text'])):

    def render_imp(self, cx):
        cx.line(self.text.value)

class AstMacro(namedtuple('AstMacro', ['name', 'bindVars', 'closeHead', 'macroBody', 'endMacro'])):

    def render_imp(self, cx):
        pass

    def invoke_imp(self, cx):
        indent = '  ' * cx.d_level

        if self.closeHead.value.startswith('---'): trimLeft = 3
        elif self.closeHead.value.startswith('--'): trimLeft = 2
        elif self.closeHead.value.startswith('-'): trimLeft = 1
        elif self.closeHead.value.startswith('+'): trimLeft = 0
        else: trimLeft = 2

        x = self.endMacro.value.split()[0]
        if x.endswith('---'): trimRight = 3
        elif x.endswith('--'): trimRight = 2
        elif x.endswith('-'): trimRight = 1
        elif x.endswith('+'): trimRight = 0
        else: trimRight = 2

        cx.d_level += 1 # TODO use a guard
        cx.d_innerLeftTrim.append(trimLeft) # TODO use a guard
        cx.d_innerRightTrim.append(trimRight)
        cx.line(indent + ", (")
        self.macroBody.render_imp(cx)
        cx.line(indent + ')')
        cx.d_level -= 1
        cx.d_innerLeftTrim.pop()
        cx.d_innerRightTrim.pop()

    def outerTrimLeft(self):
        return 0

    def outerTrimRight(self):
        return 0

class AstBody(namedtuple('AstBody', ['nodes'])):

    def render_imp(self, cx):
        indent = '  ' * cx.d_level
        ufmt = ''

        leftTrim = { }
        rightTrim = { }

        inserts = []
        selects = []
        for node in self.nodes:
            if isinstance(node, AstSql) and node.hasInsert:
                inserts.append(node)
            elif isinstance(node, AstMacro):
                cx.registerMacro(node)
            else:
                if isinstance(node, AstText):
                    if len(selects) and isinstance(selects[-1], AstText):
                        selects[-1] = selects[-1].concat(node)
                    else:
                        selects.append(node)
                else:
                    selects.append(node)

        ctx = selects[0]
        for node in selects[1:]:
            # Compute rightTrim:
            if isinstance(ctx, AstText):
                rightTrim[ctx] = node.outerTrimLeft()
            # Compute leftTrim:
            if isinstance(node, AstText):
                leftTrim[node] = ctx.outerTrimRight()
            ctx = node

        for node in inserts:
            node.render_imp(cx)
            cx.line(';')
            cx.line('')

        for node in selects:
            if isinstance(node, AstText):
                txt = node.text.value
                txt = TextUtil.ltrim(txt, leftTrim.get(node, 0))
                txt = TextUtil.rtrim(txt, rightTrim.get(node, 0))
                txt = txt.replace('%', '%%')
                ufmt += txt
            elif isinstance(node, AstSql):
                ufmt += '%s'
            elif isinstance(node, AstEscape):
                ufmt += '%s'
            else:
                print(node, file=sys.stderr)
                assert(not "Shouldn't be here.")

        ufmt = TextUtil.ltrim(ufmt, cx.innerLeftTrim())
        ufmt = TextUtil.rtrim(ufmt, cx.innerRightTrim())

        fmt = TextUtil.sqliteQuote(ufmt)
        cx.line(indent + 'SELECT printf(' + fmt)
        for node in selects:
            if isinstance(node, AstText):
                pass
            else:
                cx.d_level += 1 # TODO use a guard
                node.render_imp(cx)
                cx.d_level -= 1

        cx.line(indent + ') _pp')
        if 0 == cx.d_level:
            cx.line(';')

class AstText(namedtuple('AstText', ['text'])):
    def concat(self, other):
        return AstText(text=Token(
            kind=self.text.kind,
            strKind=self.text.strKind,
            value=self.text.value + other.text.value,
            start=self.text.start,
            end=self.text.end))

class AstSql(namedtuple('AstSql', ['hasInsert', 'sqlOpen', 'sql', 'sqlClose', 'sep', 'body', 'sqlEnd'])):
    def render_imp(self, cx):
        indent = '  ' * cx.d_level
        cx.d_level += 1 # TODO use a guard
        cx.d_innerLeftTrim.append(self.innerTrimRight()) # TODO use a guard
        cx.d_innerRightTrim.append(self.innerTrimLeft())
        cx.d_aliases.append(cx.d_aliases[-1].copy())
        x = []

        foundFrom = False
        for part in self.sql:
            if part.kind == Parser.e_TOK_HYGIENICALIAS:
                if foundFrom:
                    cx.createAlias(part)
            elif part.kind == Parser.e_TOK_TEXT:
                if 'FROM' == part.value:
                    foundFrom = True
                elif 'INSERT' == part.value:
                    if foundFrom:
                        raise RuntimeError("'INSERT' must go before 'FROM'")

        for part in self.sql:
            if (part.kind == Parser.e_TOK_BINDVAR):
                x.append(cx.getBindValue(part))
            elif (part.kind == Parser.e_TOK_HYGIENICALIAS):
                x.append(cx.getAliasValue(part))
            elif (part.kind == Parser.e_TOK_TEXT):
                x.append(part.value)
            else:
                print(part, file=sys.stderr)
                assert(not "Shouldn't be here.")

        sql = ''.join(x).strip()

        # Handle INSERT:
        if sql.startswith('INSERT'):
            # TODO / FIXME This won't handle parenthesized expressions:
            m = re.match(r'INSERT\s+INTO\s+(?P<TABLE>[a-zA-Z0-9_]+)\s+\((?P<COLUMNS>[^)]*)\)\s+VALUES\s+\((?P<VALUES>[^)]*)\)(\s+|$)', sql)
            if not m:
                raise RuntimeError("Improperly formatted 'INSERT' block.")
            g = m.groupdict()
            insert = 'INSERT INTO %s (%s) ' % (g['TABLE'], g['COLUMNS'])
            values = [x.strip() for x in g['VALUES'].split(',')]
            sqlFrom = sql[m.end():]
        else:
            insert = None
            sqlFrom = sql

        assert(not sqlFrom or sqlFrom.startswith('FROM'))

        if insert:
            cx.line(insert)
            cx.line(indent + 'SELECT ')
            for i, value in enumerate(values):
                if i == 0:
                    comma = ''
                else:
                    comma = ', '
                if '$$' == value:
                    cx.line(indent + comma + '(')
                    self.body.render_imp(cx)
                    cx.line(indent + ')')
                else:
                    cx.line(indent + comma + value)
        else:
            cx.line(indent + ", (SELECT group_concat(_pp, " + self.sep + ") FROM (")
            self.body.render_imp(cx)
        if sqlFrom:
            cx.d_lines += [indent + '  ' + x.strip() for x in sqlFrom.splitlines()]
        if insert:
            pass
        else:
            cx.line(indent + '  ))')
        cx.d_aliases.pop()
        cx.d_innerLeftTrim.pop()
        cx.d_innerRightTrim.pop()
        cx.d_level -= 1

    def outerTrimLeft(self):
        # TODO the parser or tokenizer should extract this:
        if self.sqlOpen.value.endswith('---'): return 3
        if self.sqlOpen.value.endswith('--'): return 2
        if self.sqlOpen.value.endswith('-'): return 1
        if self.sqlOpen.value.endswith('+'): return 0
        return 1

    def innerTrimRight(self):
        if self.sqlClose.value.startswith('---'): return 3
        if self.sqlClose.value.startswith('--'): return 2
        if self.sqlClose.value.startswith('-'): return 1
        if self.sqlClose.value.startswith('+'): return 0
        return 2

    def innerTrimLeft(self):
        x = self.sqlEnd.value.split()[0]
        if x.endswith('---'): return 3
        if x.endswith('--'): return 2
        if x.endswith('-'): return 1
        if x.endswith('+'): return 0
        return 1

    def outerTrimRight(self):
        x = self.sqlEnd.value.split()[-1]
        if x.startswith('---'): return 3
        if x.startswith('--'): return 2
        if x.startswith('-'): return 1
        if x.startswith('+'): return 0
        return 2

class AstEscape(namedtuple('AstEscape', ['escapeParts'])):
    def render_imp(self, cx):
        call = None
        if self.escapeParts and self.escapeParts[0].kind == Parser.e_TOK_TEXT:
            pp = re.split(r'[\s(]+', self.escapeParts[0].value.strip())
            if len(pp) >= 2:
                callKeyword, callee = pp[0:2]
                if 'call' == callKeyword:
                    call = callee
        x = []
        for part in self.escapeParts:
            if (part.kind == Parser.e_TOK_BINDVAR):
                x.append(cx.getBindValue(part))
            elif (part.kind == Parser.e_TOK_HYGIENICALIAS):
                x.append(cx.getAliasValue(part))
            elif (part.kind == Parser.e_TOK_TEXT):
                x.append(part.value)
            else:
                print(part, file=sys.stderr)
                assert(not "Shouldn't be here.")
        indent = '  ' * cx.d_level
        sqlExpr = ''.join(x).strip()
        if not call:
            cx.line(indent + ', ' + sqlExpr)
        else:
            i = sqlExpr.find('(')
            j = sqlExpr.rfind(')')
            if i < 0 or j < 0:
                raise RuntimeError("Invalid escape expression: " + repr(self))
            i += 1
            # TODO / FIXME: Support comma in subexpressions and strings:
            pp = [y for y in (x.strip() for x in sqlExpr[i:j].split(',')) if y]
            cx.invokeMacro(call, pp)

    def outerTrimLeft(self):
        return 0

    def outerTrimRight(self):
        return 0


class Parser:
    e_DONE = 1
    e_INIT = 2
    e_FINI = 3
    e_CODE = 4
    e_SQL = 5
    e_MACRO = 6
    e_ESCAPE = 7
    e_COMMENT = 8

    d_sectionSeparator = re.escape('%%')
    d_blockOpen = re.escape('{%')
    d_blockClose = re.escape('%}')
    d_escapeOpen = re.escape('{{')
    d_escapeClose = re.escape('}}')
    d_commentOpen = re.escape('{#')
    d_commentClose = re.escape('#}')

    e_TOK_SEPINIT       = 11
    e_TOK_SEPFINI       = 12
    e_TOK_SEPCODE       = 13
    e_TOK_SEPDONE       = 14
    e_TOK_SEPSEP        = 15
    e_TOK_INIT          = 16
    e_TOK_CLEANUP       = 17
    e_TOK_SQLOPEN       = 18
    e_TOK_MACROOPEN     = 19
    e_TOK_ENDSQL        = 20
    e_TOK_ENDMACRO      = 21
    e_TOK_ERROR         = 22
    e_TOK_ESCAPEOPEN    = 23
    e_TOK_SQLCLOSE      = 24
    e_TOK_MACROCLOSE    = 25
    e_TOK_ESCAPECLOSE   = 26
    e_TOK_TEXT          = 27
    e_TOK_LPAREN        = 31
    e_TOK_RPAREN        = 32
    e_TOK_COMMA         = 33
    e_TOK_WHITESPACE    = 34
    e_TOK_BINDVAR       = 35
    e_TOK_ID            = 36
    e_TOK_HYGIENICALIAS = 37
    e_TOK_LINE          = 38
    e_TOK_IGNORE        = 39
    e_TOK_COMMENTOPEN   = 40
    e_TOK_COMMENTCLOSE  = 41
    e_TOK_EOF           = 100

    d_stateStr = {
        e_DONE    : 'DONE',
        e_INIT    : 'INIT',
        e_FINI    : 'FINI',
        e_CODE    : 'CODE',
        e_SQL     : 'SQL',
        e_MACRO   : 'MACRO',
        e_ESCAPE  : 'ESCAPE',
        e_COMMENT : 'COMMENT',
    }

    d_strState = dict((v, k) for k, v in d_stateStr.items())

    d_tokStr = {
        e_TOK_SEPINIT       : 'TOK_SEPINIT',
        e_TOK_SEPFINI       : 'TOK_SEPFINI',
        e_TOK_SEPCODE       : 'TOK_SEPCODE',
        e_TOK_SEPDONE       : 'TOK_SEPDONE',
        e_TOK_SEPSEP        : 'TOK_SEPSEP',
        e_TOK_INIT          : 'TOK_INIT',
        e_TOK_CLEANUP       : 'TOK_CLEANUP',
        e_TOK_SQLOPEN       : 'TOK_SQLOPEN',
        e_TOK_MACROOPEN     : 'TOK_MACROOPEN',
        e_TOK_ENDSQL        : 'TOK_ENDSQL',
        e_TOK_ENDMACRO      : 'TOK_ENDMACRO',
        e_TOK_ERROR         : 'TOK_ERROR',
        e_TOK_ESCAPEOPEN    : 'TOK_ESCAPEOPEN',
        e_TOK_SQLCLOSE      : 'TOK_SQLCLOSE',
        e_TOK_MACROCLOSE    : 'TOK_MACROCLOSE',
        e_TOK_ESCAPECLOSE   : 'TOK_ESCAPECLOSE',
        e_TOK_TEXT          : 'TOK_TEXT',
        e_TOK_LPAREN        : 'TOK_LPAREN',
        e_TOK_RPAREN        : 'TOK_RPAREN',
        e_TOK_COMMA         : 'TOK_COMMA',
        e_TOK_WHITESPACE    : 'TOK_WHITESPACE',
        e_TOK_BINDVAR       : 'TOK_BINDVAR',
        e_TOK_ID            : 'TOK_ID',
        e_TOK_HYGIENICALIAS : 'TOK_HYGIENICALIAS',
        e_TOK_LINE          : 'TOK_LINE',
        e_TOK_IGNORE        : 'TOK_IGNORE',
        e_TOK_COMMENTOPEN   : 'TOK_COMMENTOPEN',
        e_TOK_COMMENTCLOSE  : 'TOK_COMMENTCLOSE',
        e_TOK_EOF           : 'TOK_EOF',
    }

    d_strTok = dict((v, k) for k, v in d_tokStr.items())

    d_lineRe = r'(?P<TOK_LINE>^#.*$)'

    d_sepRes = \
        d_lineRe + '|' + \
        '(?P<TOK_SEPINIT>^\s*' + d_sectionSeparator + ' init$)|' + \
        '(?P<TOK_SEPFINI>^\s*' + d_sectionSeparator + ' fini$)|' + \
        '(?P<TOK_SEPCODE>^\s*' + d_sectionSeparator + ' code$)|' + \
        '(?P<TOK_SEPDONE>^\s*' + d_sectionSeparator + ' done$)|' + \
        '(?P<TOK_SEPSEP>^\s*' + d_sectionSeparator + ' [^a-zA-Z0-9_].*$)'

    d_hygienicAliasRe = r'(?P<TOK_HYGIENICALIAS>\$[A-Z][a-zA-Z0-9_]*\b)'
    d_bindVarRe = r'(?P<TOK_BINDVAR>@[a-z][a-zA-Z0-9_]*\b)'
    d_idRe = r'(?P<TOK_ID>\b[a-z][a-zA-Z0-9_]*\b)'

    d_rules = {
        e_DONE: re.compile(d_sepRes, re.M),
        e_INIT: re.compile(d_sepRes, re.M),
        e_FINI: re.compile(d_sepRes, re.M),
        e_CODE: re.compile('|'.join([
            '(?P<TOK_SQLOPEN>' + d_blockOpen + r'[-+]{0,3})\s*(INSERT|FROM)\b',
            '(?P<TOK_MACROOPEN>' + d_blockOpen + r')\s*macro\b',
            '(?P<TOK_ENDSQL>' + d_blockOpen + r'[-+]{0,3}\s*END\s*[-+]{0,3}' + d_blockClose + ')',
            '(?P<TOK_ENDMACRO>' + d_blockOpen + r'[-+]{0,3}\s*endmacro\s*' + d_blockClose + ')',
            '(?P<TOK_ERROR>' + d_blockOpen + r'-*)',
            '(?P<TOK_ESCAPEOPEN>' + d_escapeOpen + ')',
            '(?P<TOK_COMMENTOPEN>' + d_commentOpen + ')',
            '(?P<TOK_COMMENTCLOSE>' + d_commentClose + ')',
            d_sepRes]), re.M),
        e_SQL: re.compile('|'.join([
            # TODO "(?P<TOK_TEXT>'(?:''|[^'])*')",
            r'(?P<TOK_TEXT>\b(INSERT|FROM)\b)',
            d_hygienicAliasRe,
            d_bindVarRe,
            '(?P<TOK_SQLCLOSE>[-+]{0,3}' + d_blockClose + ')'])),
        e_MACRO: re.compile('|'.join([
            '(?P<TOK_LPAREN>[(])',
            '(?P<TOK_RPAREN>[)])',
            '(?P<TOK_COMMA>[,])',
            '(?P<TOK_WHITESPACE>\s+)',
            d_bindVarRe,
            d_idRe,
            '(?P<TOK_MACROCLOSE>[-+]{0,3}' + d_blockClose + ')'])),
        e_ESCAPE: re.compile('|'.join([
            d_hygienicAliasRe,
            d_bindVarRe,
            '(?P<TOK_ESCAPECLOSE>' + d_escapeClose + ')'])),
        e_COMMENT: re.compile('|'.join([
            '(?P<TOK_COMMENTOPEN>' + d_commentOpen + ')',
            '(?P<TOK_COMMENTCLOSE>' + d_commentClose + ')'])),
    }

    def __init__(self, trace=False):
        self.d_state = self.e_DONE
        self.d_input = ''
        self.d_commentCount = 0
        self.d_offset = 0
        self.d_sqlLevel = 0
        self.d_tokens = []
        self.d_nextToken = None
        self.d_currentToken = None
        self.d_trace = trace

    def tokens(self):
        while True:
            if self.d_offset >= len(self.d_input):
                break
            if not len(self.d_input):
                break

            m = self.d_rules[self.d_state].search(self.d_input, pos=self.d_offset)
            if m is None:
                if self.d_state != self.e_DONE:
                    print('d_state =', self.d_state, file=sys.stderr)
                    raise RuntimeError('Lexer error: unexpected end of input.')
                tokKind = self.e_TOK_TEXT
                tokValue = self.d_input[self.d_offset:]
                yield Token(
                    kind=tokKind, strKind=self.d_tokStr[tokKind],
                    value=tokValue, start=self.d_offset, end=len(self.d_input))
                break

            # Rules for interstitial text (text that doesn't match a regex we
            # care about):
            if self.d_offset != m.start():
                if self.d_state == self.e_INIT:
                    tokKind = self.e_TOK_TEXT
                elif self.d_state == self.e_FINI:
                    tokKind = self.e_TOK_TEXT
                elif self.d_state == self.e_CODE:
                    tokKind = self.e_TOK_TEXT
                elif self.d_state == self.e_SQL:
                    tokKind = self.e_TOK_TEXT
                elif self.d_state == self.e_ESCAPE:
                    tokKind = self.e_TOK_TEXT
                elif self.d_state == self.e_DONE:
                    tokKind = self.e_TOK_IGNORE
                elif self.d_state == self.e_COMMENT:
                    tokKind = self.e_TOK_IGNORE
                else:
                    assert(not "Shouldn't be here.")
                tokValue = self.d_input[self.d_offset:m.start()]
                yield Token(
                    kind=tokKind, strKind=self.d_tokStr[tokKind],
                    value=tokValue, start=self.d_offset, end=m.start())

            mk = [(k, v) for k, v in m.groupdict().items() if v is not None]
            if 1 != len(mk):
                raise RuntimeError("Internal lexer error.")

            strTokKind, tokValue = mk[0]
            tokKind = self.d_strTok[strTokKind]
            yield Token(
                kind=tokKind, strKind=self.d_tokStr[tokKind], value=tokValue,
                start=m.start(), end=m.start() + len(tokValue))

            self.d_offset = m.start() + len(tokValue)

            nextState = self.d_state

            # Rules for noninterstitial text:
            if False: # so that the subsequent blocks are written uniformly
                pass

            elif self.d_state == self.e_DONE:
                if self.e_TOK_SEPINIT == tokKind:
                    nextState = self.e_INIT
                elif self.e_TOK_SEPFINI == tokKind:
                    nextState = self.e_FINI
                elif self.e_TOK_SEPCODE == tokKind:
                    nextState = self.e_CODE
                elif self.e_TOK_SEPDONE == tokKind:
                    nextState = self.e_DONE

            elif self.d_state == self.e_INIT:
                if self.e_TOK_SEPINIT == tokKind:
                    nextState = self.e_INIT
                elif self.e_TOK_SEPFINI == tokKind:
                    nextState = self.e_FINI
                elif self.e_TOK_SEPCODE == tokKind:
                    nextState = self.e_CODE
                elif self.e_TOK_SEPDONE == tokKind:
                    nextState = self.e_DONE

            elif self.d_state == self.e_FINI:
                if self.e_TOK_SEPINIT == tokKind:
                    nextState = self.e_INIT
                elif self.e_TOK_SEPFINI == tokKind:
                    nextState = self.e_FINI
                elif self.e_TOK_SEPCODE == tokKind:
                    nextState = self.e_CODE
                elif self.e_TOK_SEPDONE == tokKind:
                    nextState = self.e_DONE

            elif self.d_state == self.e_CODE:
                if self.e_TOK_SQLOPEN == tokKind:
                    nextState = self.e_SQL
                elif self.e_TOK_MACROOPEN == tokKind:
                    nextState = self.e_MACRO
                elif self.e_TOK_ESCAPEOPEN == tokKind:
                    nextState = self.e_ESCAPE
                elif self.e_TOK_COMMENTOPEN == tokKind:
                    self.d_commentCount += 1
                    nextState = self.e_COMMENT
                elif self.e_TOK_SEPINIT == tokKind:
                    nextState = self.e_INIT
                elif self.e_TOK_SEPFINI == tokKind:
                    nextState = self.e_FINI
                elif self.e_TOK_SEPCODE == tokKind:
                    nextState = self.e_CODE
                elif self.e_TOK_SEPDONE == tokKind:
                    nextState = self.e_DONE
                elif self.e_TOK_COMMENTCLOSE == tokKind:
                    raise RuntimeError("Encountered invalid token: " + repr(Token(
                        kind=tokKind, strKind=self.d_tokStr[tokKind],
                        value=tokValue, start=len(self.d_input), end=len(self.d_input))))

            elif self.d_state == self.e_SQL:
                if self.e_TOK_SQLCLOSE == tokKind:
                    nextState = self.e_CODE

            elif self.d_state == self.e_MACRO:
                if self.e_TOK_MACROCLOSE == tokKind:
                    nextState = self.e_CODE

            elif self.d_state == self.e_ESCAPE:
                if self.e_TOK_ESCAPECLOSE == tokKind:
                    nextState = self.e_CODE

            elif self.d_state == self.e_COMMENT:
                if self.e_TOK_COMMENTOPEN == tokKind:
                    self.d_commentCount += 1
                    nextState = nextState # NO CHANGE
                elif self.e_TOK_COMMENTCLOSE == tokKind:
                    self.d_commentCount -= 1
                    if 0 == self.d_commentCount:
                        nextState = self.e_CODE

            if self.d_trace and self.d_state != nextState:
                print('STATE', self.d_stateStr[self.d_state], \
                    '->', self.d_stateStr[nextState], file=sys.stderr)
            self.d_state = nextState

        tokKind = self.e_TOK_EOF
        tokValue = ''
        yield Token(
            kind=tokKind, strKind=self.d_tokStr[tokKind],
            value=tokValue, start=len(self.d_input), end=len(self.d_input))

    def token(self, kindCheck=None, noSkipWhitespace=False, noSkipLine=False):
        while True:
            if self.d_tokens:
                tok = self.d_tokens.pop()
            else:
                tok = next(self.d_nextToken)
            if self.d_trace:
                print('TOKEN', tok, file=sys.stderr)
            if (noSkipWhitespace or tok.kind != self.e_TOK_WHITESPACE) and \
               (noSkipLine or tok.kind != self.e_TOK_LINE) and \
               tok.kind != self.e_TOK_IGNORE and \
               tok.kind != self.e_TOK_COMMENTOPEN and \
               tok.kind != self.e_TOK_COMMENTCLOSE:
                if kindCheck is not None and tok.kind != kindCheck:
                    raise ParseError(self.d_tokStr[kindCheck], tok)
                return tok

    def peek(self):
        if not self.d_tokens:
            self.d_tokens.append(self.token())
        return self.d_tokens[-1]

    def parseSql_imp(self):
        if self.d_trace:
            print('ENTER parseSql_imp()', file=sys.stderr)
        # TODO join=''
        sqlOpen = self.token(self.e_TOK_SQLOPEN)

        hasInsert = False
        sqlTokens = []
        while True:
            tt = self.peek()
            if tt.kind == self.e_TOK_SQLCLOSE:
                break
            tt = self.token()
            if tt.kind == self.e_TOK_TEXT:
                if 'INSERT' == tt.value:
                    hasInsert = True
                sqlTokens.append(tt)
            elif tt.kind == self.e_TOK_BINDVAR:
                # FIXME: Don't tokenize if inside quoted string.
                sqlTokens.append(tt)
            elif tt.kind == self.e_TOK_HYGIENICALIAS:
                # FIXME: Don't tokenize if inside quoted string.
                sqlTokens.append(tt)
            else:
                print(tt, file=sys.stderr)
                assert(not "Shouldn't be here.")

        sep = "''"

        # XXX unkludge this (extension of an island grammar):
        last = sqlTokens[-1]
        if last.kind == self.e_TOK_TEXT:
            sqlt = TextUtil.sqlSplit(last.value.rstrip())
            if len(sqlt) >= 3 and 'SEP' == sqlt[-3]:
                assert(sqlt[-1][0] in ("'",))
                sep = sqlt[-1][1:-1]
                sep = sep.replace("''", "'")
                sep = sep.replace("\\n", "\n")
                sep = TextUtil.sqliteQuote(sep)
                tokValue = ''.join(sqlt[:-3])
                # TODO rewrite end:
                sqlTokens[-1] = Token(
                    kind=last.kind, strKind=last.strKind,
                    value=tokValue, start=last.start, end=last.end)
                # TODO generate a new token for "SEP"

        sqlClose = self.token(self.e_TOK_SQLCLOSE)
        body = self.parseBody_imp()
        sqlEnd = self.token(self.e_TOK_ENDSQL)
        if self.d_trace:
            print('LEAVE parseSql_imp()', file=sys.stderr)
        return AstSql(hasInsert, sqlOpen, sqlTokens, sqlClose, sep, body, sqlEnd)

    def parseMacro_imp(self):
        self.token(self.e_TOK_MACROOPEN)
        macroKeyword = self.token(self.e_TOK_ID)
        assert(macroKeyword.value == 'macro')
        name = self.token(self.e_TOK_ID)
        self.token(self.e_TOK_LPAREN)
        bindVars = []
        tt = self.peek()
        while tt.kind != self.e_TOK_RPAREN:
            bindVars.append(self.token(self.e_TOK_BINDVAR))
            tt = self.peek()
            if tt.kind == self.e_TOK_COMMA:
                self.token(self.e_TOK_COMMA)
        self.token(self.e_TOK_RPAREN)
        closeHead = self.token(self.e_TOK_MACROCLOSE)
        macroBody = self.parseBody_imp()
        endMacro = self.token(self.e_TOK_ENDMACRO)
        return AstMacro(name, bindVars, closeHead, macroBody, endMacro)

    def parseEscape_imp(self):
        self.token(self.e_TOK_ESCAPEOPEN)

        escapeParts = []
        while True:
            tt = self.peek()
            if tt.kind == self.e_TOK_ESCAPECLOSE:
                break
            else:
                tt = self.token()
                escapeParts.append(tt)

        tt = self.token(self.e_TOK_ESCAPECLOSE)
        return AstEscape(escapeParts)

    def parseBody_imp(self):
        if self.d_trace:
            print('ENTER parseBody_imp()', file=sys.stderr)
        nodes = []
        while True:
            tt = self.peek()
            while tt.kind == self.e_TOK_TEXT:
                self.token(self.e_TOK_TEXT)
                nodes.append(AstText(tt))
                tt = self.peek()

            if False: pass
            elif tt.kind == self.e_TOK_SQLOPEN: nodes.append(self.parseSql_imp())
            elif tt.kind == self.e_TOK_MACROOPEN: nodes.append(self.parseMacro_imp())
            elif tt.kind == self.e_TOK_ESCAPEOPEN: nodes.append(self.parseEscape_imp())
            elif tt.kind == self.e_TOK_EOF: break
            else:
                break
        if self.d_trace:
            print('LEAVE parseBody_imp()', file=sys.stderr)
        return AstBody(nodes)

    def parseInit_imp(self):
        text = self.token(self.e_TOK_TEXT)
        return AstInit(text)

    def parseFini_imp(self):
        text = self.token(self.e_TOK_TEXT)
        return AstFini(text)

    def parseTop_imp(self):
        sections = []
        while True:
            tt = self.peek()
            if tt.kind == self.e_TOK_SEPINIT:
                self.token(self.e_TOK_SEPINIT)
                sections.append(self.parseInit_imp())
            elif tt.kind == self.e_TOK_SEPFINI:
                self.token(self.e_TOK_SEPFINI)
                sections.append(self.parseFini_imp())
            elif tt.kind == self.e_TOK_SEPCODE:
                self.token(self.e_TOK_SEPCODE)
                sections.append(self.parseBody_imp())
            elif tt.kind == self.e_TOK_SEPDONE:
                self.token(self.e_TOK_SEPDONE)
            elif tt.kind == self.e_TOK_TEXT:
                self.token(self.e_TOK_TEXT)
            else:
                break
        return AstTop(sections=sections)

    def parse_imp(self):
        ast = None
        ast = self.parseTop_imp()
        return ast

    def parse(self, text):
        self.d_input = text
        self.d_nextToken = self.tokens()
        self.d_tokens = []
        ast = self.parse_imp()
        self.token(self.e_TOK_EOF)
        return ast

def main(out, argv):
    options = Options.fromArgv(argv[1:])

    if options.noMcpp:
        with open(options.source, 'rb') as f:
            text = f.read().decode("utf-8")
    else:
        mcpp = options.findExecutable(os.environ.get('MCPP', 'mcpp'))
        mcppIncludePaths = ['-I' + ii for ii in options.includePaths]
        cmd = [mcpp] + mcppIncludePaths + [options.source]
        ps = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        text = ps.stdout.read().decode("utf-8")
        rc = ps.wait()
        if 0 != rc:
            raise RuntimeError("Error running mcpp (rc=%d): %s" % (
                rc, repr(cmd)
            ))

    if options.noRender:
        print(text, file=out)
    else:
        parser = Parser(trace=False)
        ast = parser.parse(text)
        #print simplejson.dumps(ast, indent=2, namedtuple_as_object=True)
        lines = []
        with FileUtil.tempdir() as d:
            useSysDb = True
            sysDb = options.sysDb or os.path.join(d, 'sys.db')
            if useSysDb:
                # FIXME nondeterministic output:
                lines.append("ATTACH DATABASE %s AS sys;" % (
                    TextUtil.sqliteQuote(sysDb)))
                lines.append("DROP TABLE IF EXISTS sys.sys_Write;")
                lines.append("CREATE TABLE sys.sys_Write ( path UNIQUE, content );")
            else:
                lines.append("DROP TABLE IF EXISTS sys_Write;")
                lines.append("CREATE TABLE sys_Write ( path UNIQUE, content );")
            lines.append(ast.render())
            sqlText = '\n'.join(lines)
            if options.noSqlite3:
                print(sqlText, file=out)
            else:
                sqlFilename = os.path.join(d, 'script.sql')
                with open(sqlFilename, 'wb') as f:
                    f.write(sqlText.encode("utf-8"))
                sqlite3Cmd = options.findExecutable(os.environ.get(
                    'SQLITE3', 'sqlite3'))
                cmd = [sqlite3Cmd]
                if options.db:
                    cmd.append(options.db)
                with open(sqlFilename, 'rb') as f:
                    ps = subprocess.Popen(cmd, stdin=f, stdout=subprocess.PIPE)
                result = ps.stdout.read().decode("utf-8")
                rc = ps.wait()
                if 0 != rc:
                    raise RuntimeError("Error running sqlite3 (rc=%d): %s" % (
                        rc, repr(cmd)
                    ))
                print(result, file=out)

                db = sqlite3.connect(sysDb)
                c = db.cursor()

                toWrite = [(os.path.normpath(path), path, content)
                    for path, content in
                    c.execute("SELECT path, content FROM sys_Write")]

                # Validate:
                validPathCharRe = re.compile('^[-_./a-zA-Z0-9]+$')
                directoryEscapeRe = re.compile('/[.]+/')
                for normpath, path, _ in toWrite:
                    if not len(path):
                        raise RuntimeError('sys_Write path must not be empty: ' + repr(path))
                    if not validPathCharRe.match(path):
                        raise RuntimeError('sys_Write path has invalid character: ' + repr(path))
                    if path[0] == '/':
                        raise RuntimeError('sys_Write path must be relative: ' + repr(path))
                    if directoryEscapeRe.search(path):
                        raise RuntimeError('sys_Write path must not use . or .., et al.: ' + repr(path))
                    # Do a final system sanity check:
                    if os.path.isabs(normpath) or normpath.startswith('..') \
                       or ':' in normpath:
                        raise RuntimeError("Invalid / unsafe path in sys_Write: " + normpath)


                if options.prefix:
                    dirnames = set(os.path.join(options.prefix,
                                                os.path.dirname(normpath))
                                   for normpath, _, _ in toWrite)
                    for dirname in dirnames:
                        if dirname and not os.path.exists(dirname):
                            try:
                                os.makedirs(dirname)
                                if not options.quiet:
                                    print('Creating directory: ' + dirname, file=sys.stderr)
                            except OSError as exc: # Guard against race condition
                                if exc.errno != errno.EEXIST:
                                    raise

                    for normpath, path, content in toWrite:
                        # TODO: Write using a few threads
                        actualName = os.path.join(options.prefix, normpath)
                        with open(actualName, 'wb') as f:
                            if not options.quiet:
                                print('Writing: ' + actualName, file=sys.stderr)
                            f.write(content.encode("utf-8"))

                else:
                    if len(toWrite):
                        print('Warning: sys_Write files will not be written unless `--prefix PREFIX` is specified.', file=sys.stderr)

if '__main__' == __name__:
    main(sys.stdout, sys.argv)
