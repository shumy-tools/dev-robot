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

    predicate: path comp value ;

      path: ID next* ;

        next: deref ID ;

          deref: ('.' | '..') ;

      comp: ('==' | '!=' | '>' | '<' | '>=' | '<=' | 'in') ;

      value: (INT | STRING) ;

  limit: 'limit' INT ;

  page: 'page' INT;

  select: '{' fields (',' relation)* '}' ;

    fields: ALL | (field (',' field)*) ;

      field: ('(' order INT ')')? ID ;

        order: ('asc' | 'desc') ;

    relation: ID qline ;

NAME: [A-Z] ID ;
ALL: '*' ;

ID: [_]*[a-z][A-Za-z0-9_]* ;
INT: [0-9]+ ;
STRING: '"' ( '""' | ~["\r\n] )* '"';

WS: [ \t\r\n\f]+ -> skip ;