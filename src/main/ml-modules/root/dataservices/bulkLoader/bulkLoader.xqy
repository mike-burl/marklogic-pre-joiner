xquery version "1.0-ml";

declare option xdmp:mapping "false";

declare variable $session as document-node()? external;
declare variable $endpointState as document-node()? external;
declare variable $workUnit as document-node()? external;
declare variable $input as document-node()* external;

for $doc in $input
    let $primaryID := fn:normalize-space($doc/envelope/PRIMARY_Collection/PRIMARY[1]/PRIMARY_ID/fn:string())
    let $uri := fn:concat("/new-mexico/mlps/claims/test/", $primaryID, ".xml")
    return xdmp:document-insert($uri, $doc, map:map() => map:with("collections", ("mlps-claim","mlps-test")))