grammar Query;

query: entity qline EOF ;

entity: (ID '.')* NAME ;

qline: filter? (limit page?)? select ;

  filter: '|' expr '|' ;

    expr: '(' expr ')'
      | left=expr oper='and' right=expr
      | left=expr oper='or' right=expr
      | predicate
    ;

    predicate: path comp param ;

      path: ID next* ;

        next: deref ID ;

          deref: ('.' | '..') ;

      comp: ('==' | '!=' | '>' | '<' | '>=' | '<=' | 'in') ;

      param: value | list ;

        value: TEXT | INT | FLOAT | BOOL | TIME | DATE | DATETIME | PARAM ;

        list: '[' value+ ']';

  limit: 'limit' INT ;

  page: 'page' INT;

  select: '{' fields (',' relation)* '}' ;

    fields: ALL | (field (',' field)*) ;

      field: ('(' order INT ')')? ID ;

        order: ('asc' | 'desc') ;

    relation: ID qline ;

ALL: '*' ;
NAME: [A-Z] ID ;
ID: [_]*[a-z][A-Za-z0-9_]* ;

// value types
  INT: '-'? [0-9]+ ;
  FLOAT: INT ('.' [0-9]+)? ;
  TEXT: '"' ( '""' | ~["\r\n] )* '"';
  BOOL: 'true' | 'false' ;

  TIME: '#' [0-2][0-9]':'[0-9][0-9]':'[0-9][0-9] ;
  DATE: '#' [0-9][0-9][0-9][0-9]'-'[0-9][0-9]'-'[0-9][0-9] ;
  DATETIME: DATE'T'[0-2][0-9]':'[0-9][0-9]':'[0-9][0-9] ;

  PARAM: '?' ID;

WS: [ \t\r\n\f]+ -> skip ;