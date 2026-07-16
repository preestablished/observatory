//! Minimal grid-hint extraction from the canonical feature-map schema
//! (reference-workload API.md §1). Deliberately small: full feature-map
//! validation and skip metering UX belong to M6's `obs-coverage`.
//!
//! Accepts either a JSON feature map or the canonical YAML with an
//! inline-flow `discretize: { kind: grid, ... }` block (possibly spanning
//! lines, as in the reference-workload example). Observatory never
//! invents bin parameters: no grid hint, no coverage map.

use obs_store::GridHint;

/// Finds the FIRST feature carrying a `discretize: {kind: grid, ...}`
/// hint (v1 renders the first in map order).
#[must_use]
pub fn parse_grid_hint(feature_map: &str) -> Option<GridHint> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(feature_map) {
        return parse_json(&value);
    }
    parse_flow_blocks(feature_map)
}

fn parse_json(value: &serde_json::Value) -> Option<GridHint> {
    let features = value.get("features")?.as_array()?;
    for feature in features {
        let Some(discretize) = feature.get("discretize") else {
            continue;
        };
        if discretize.get("kind").and_then(|k| k.as_str()) != Some("grid") {
            continue;
        }
        return Some(GridHint {
            x: discretize.get("x")?.as_str()?.to_owned(),
            y: discretize.get("y")?.as_str()?.to_owned(),
            room: discretize
                .get("room")
                .and_then(|r| r.as_str())
                .map(str::to_owned),
            cell_w: discretize.get("cell_w")?.as_f64()?,
            cell_h: discretize.get("cell_h")?.as_f64()?,
        });
    }
    None
}

/// Extracts `discretize: { ... }` inline-flow blocks from YAML text by
/// brace matching (the canonical schema declares the hint as a flow map).
fn parse_flow_blocks(text: &str) -> Option<GridHint> {
    let mut search_from = 0;
    while let Some(offset) = text[search_from..].find("discretize:") {
        let start = search_from + offset;
        search_from = start + "discretize:".len();
        let after = &text[search_from..];
        let open = after.find('{')?;
        let mut depth = 0usize;
        let mut end = None;
        for (i, ch) in after[open..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let end = end?;
        let block = &after[open + 1..end];
        if let Some(hint) = parse_flow_map(block) {
            return Some(hint);
        }
    }
    None
}

fn parse_flow_map(block: &str) -> Option<GridHint> {
    let mut kind = None;
    let mut x = None;
    let mut y = None;
    let mut room = None;
    let mut cell_w = None;
    let mut cell_h = None;
    for pair in block.split(',') {
        let (key, value) = pair.split_once(':')?;
        let key = key.trim();
        let value = value.trim().trim_matches(|c| c == '"' || c == '\'');
        match key {
            "kind" => kind = Some(value.to_owned()),
            "x" => x = Some(value.to_owned()),
            "y" => y = Some(value.to_owned()),
            "room" => room = Some(value.to_owned()),
            "cell_w" => cell_w = value.parse::<f64>().ok(),
            "cell_h" => cell_h = value.parse::<f64>().ok(),
            _ => {}
        }
    }
    if kind.as_deref() != Some("grid") {
        return None;
    }
    Some(GridHint {
        x: x?,
        y: y?,
        room,
        cell_w: cell_w?,
        cell_h: cell_h?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_reference_workload_yaml_shape() {
        let map = r#"
features:
  - name: player_x
    region: wram
    offset: 0x0AF6
    type: u16le
    semantics: position_x
    stability: volatile
    discretize: { kind: grid, x: player_x, y: player_y,
                  room: room_id, cell_w: 32, cell_h: 32 }
  - name: player_y
    discretize: { kind: none }
  - name: room_id
"#;
        let hint = parse_grid_hint(map).unwrap();
        assert_eq!(hint.x, "player_x");
        assert_eq!(hint.y, "player_y");
        assert_eq!(hint.room.as_deref(), Some("room_id"));
        assert_eq!(hint.cell_w, 32.0);
        assert_eq!(hint.cell_h, 32.0);
    }

    #[test]
    fn parses_json_feature_map() {
        let map = r#"{"features":[
            {"name":"cx","discretize":{"kind":"none"}},
            {"name":"px","discretize":{"kind":"grid","x":"px","y":"py","cell_w":16,"cell_h":8}}
        ]}"#;
        let hint = parse_grid_hint(map).unwrap();
        assert_eq!(hint.x, "px");
        assert_eq!(hint.room, None);
        assert_eq!(hint.cell_h, 8.0);
    }

    #[test]
    fn no_grid_hint_means_none() {
        assert_eq!(parse_grid_hint("features:\n  - name: a\n"), None);
        assert_eq!(
            parse_grid_hint(r#"{"features":[{"name":"a","discretize":{"kind":"none"}}]}"#),
            None
        );
        assert_eq!(parse_grid_hint("not a feature map at all"), None);
    }
}
