grammar Query;

query: entity qline EOF ;

entity: (ID '.')* NAME ;

qline: filter? (limit page?)? select ;

  filter: '|' predicate more* '|' ;

    predicate: path oper value ;

      path: ID next* ;

        next: deref ID ;

          deref: ('.' | '..') ;

      oper: ('==' | '!=' | '>' | '<' | '<=' | '>=' | 'in') ;

      value: (INT | STRING) ;

    more: logic predicate ;

      logic: ('or' | 'and') ;

  limit: 'limit' INT ;

  page: 'page' INT;

  select: '{' fields (',' relation)* '}' ;

    fields: ALL | (field (',' field)*) ;

      field: ('(' order INT ')')? ID ;

        order: ('asc' | 'desc') ;

    relation: ID qline ;

NAME: UPPER ID ;
ID: LOWER (NUMBER | LOWER | UPPER)* ;
INT: NUMBER+ ;

NUMBER: '0'..'9' ;
LOWER: 'a'..'z' ;
UPPER: 'A'..'Z' ;

ALL: '*' ;
STRING : '"' (' '..'~')* '"' ;

WS: [ \t\r\n\f]+ -> skip ;