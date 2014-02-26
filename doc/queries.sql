-- Get all:
-- props:
    SELECT gp.prop, vp.str, COUNT(gp.prop) AS cp FROM gd_prop AS gp JOIN v_gd_prop_str AS vp ON (gp.prop = vp.prop) GROUP BY gp.prop, vp.str ORDER BY cp;
-- vals:
    SELECT gp.val, vv.str, COUNT(gp.val) AS cv FROM gd_prop AS gp JOIN v_gd_val_str AS vv ON (gp.val = vv.val) GROUP BY gp.val, vv.str HAVING cv > 99 ORDER BY cv;

-- Show clusters:
    select gd_cluster_id, count(gd_cluster_id) as cc from gd_item_cluster group by gd_cluster_id order by cc desc;

-- Show actual properties of an item:
SELECT
    gp.gd_item_id,
    gp.prop,
    vp.str,
    gp.val,
    vv.str
FROM
    gd_prop            AS gp
    JOIN v_gd_prop_str AS vp ON (gp.prop = vp.prop)
    JOIN v_gd_val_str  AS vv ON (gp.val  = vv.val)
WHERE
    gp.gd_item_id = ?

-- Show the titles for a cluster:
SELECT
    gp.gd_item_id,
    gic.gd_cluster_id,
    vp.str AS prop,
    vv.str AS val
FROM
    gd_item_cluster    AS gic
    JOIN gd_prop       AS gp ON (gic.gd_item_id = gp.gd_item_id)
    JOIN v_gd_prop_str AS vp ON (gp.prop = vp.prop)
    JOIN v_gd_val_str  AS vv ON (gp.val = vv.val)
WHERE
    gic.gd_cluster_id = 18
    AND
    vp.str = 'Title';

-- Show strings for a property-value pair (mostly for human eyes):
SELECT 'prop' AS str_type, str FROM v_gd_prop_str WHERE prop = 18 
UNION 
SELECT 'val' AS str_type, str FROM v_gd_val_str WHERE val = 2046;
