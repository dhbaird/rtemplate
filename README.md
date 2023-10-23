# rtemplate

(Proof of concept) rtemplate: a template language that compiles to SQL

To run a demo that demonstrates SQL data initialization and template macros and rendering:
```
python rtemplate.py demo.tmpl
# >>>
 digraph {
     c -> b;
     b -> a;
 }
```

If you want to see the raw SQL code, run this:
```
python rtemplate.py --no-sqlite3 demo.tmpl
# >>>
ATTACH DATABASE '/tmp/reltpl4irasmmy/sys.db' AS sys;
DROP TABLE IF EXISTS sys.sys_Write;
CREATE TABLE sys.sys_Write ( path UNIQUE, content );

-- The init section contains raw sqlite code that executes before the template
CREATE TEMP TABLE Edge ( up, dn );
INSERT INTO Edge VALUES ('c', 'b');
INSERT INTO Edge VALUES ('b', 'a');

CREATE TABLE Foo (up, content);

SELECT printf('%s'
  , (
    SELECT printf('digraph {' || x'0a' || '%s}'
      , (SELECT group_concat(_pp, '') FROM (
        SELECT printf('    %s'
          , (
            SELECT printf('%s -> %s;' || x'0a' || ''
              , _4_E.up
              , _4_E.dn
            ) _pp
          )
        ) _pp
        FROM Edge _4_E ORDER BY up DESC
        ))
    ) _pp
  )
) _pp
;

-- The fini section contains raw sqlite code that executes after the template
DROP TABLE Edge;
```
