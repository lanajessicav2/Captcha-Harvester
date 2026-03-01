import os, json, time, threading, requests
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import pg8000.native

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL")
NOPECHA_KEY  = os.environ.get("NOPECHA_KEY")
ADMIN_KEY    = os.environ.get("ADMIN_KEY")

def get_db():
    # Parse postgresql://user:pass@host:port/dbname
    url = DATABASE_URL.replace("postgresql://", "").replace("postgres://", "")
    userpass, rest = url.split("@", 1)
    user, password = userpass.split(":", 1)
    hostport, dbname = rest.split("/", 1)
    host, port = (hostport.split(":", 1) if ":" in hostport else (hostport, "5432"))
    return pg8000.native.Connection(user=user, password=password, host=host, port=int(port), database=dbname, ssl_context=True)

def init_db():
    con = get_db()
    con.run("""
        CREATE TABLE IF NOT EXISTS captures (
            id          SERIAL PRIMARY KEY,
            variant     TEXT NOT NULL,
            instruction TEXT,
            images      TEXT NOT NULL,
            combined    TEXT,
            label       INTEGER,
            labeled     BOOLEAN DEFAULT FALSE,
            source      TEXT DEFAULT 'extension',
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    con.run("CREATE INDEX IF NOT EXISTS idx_variant ON captures(variant)")
    con.run("CREATE INDEX IF NOT EXISTS idx_labeled ON captures(labeled)")
    con.close()
    print("DB initialized")

def nopecha_label(combined_b64, instruction):
    img = combined_b64.split(',')[1] if ',' in combined_b64 else combined_b64
    try:
        r = requests.post('https://api.nopecha.com/', json={
            'type': 'funcaptcha', 'key': NOPECHA_KEY,
            'image_data': [img], 'task': instruction or 'Pick the image'
        }, timeout=15)
        data = r.json()
        if data.get('error') or not data.get('data'):
            return None
        job_id = data['data']
        for _ in range(40):
            time.sleep(0.6)
            d2 = requests.get(f'https://api.nopecha.com/?key={NOPECHA_KEY}&id={job_id}', timeout=10).json()
            if d2.get('error') == 14: continue
            if d2.get('error'): return None
            if isinstance(d2.get('data'), list):
                return next((i for i, v in enumerate(d2['data']) if v is True), None)
        return None
    except Exception as e:
        print(f"NopeCHA error: {e}")
        return None

def label_worker(capture_id, combined, instruction):
    label = nopecha_label(combined, instruction)
    if label is None: return
    try:
        con = get_db()
        con.run("UPDATE captures SET label=:label, labeled=TRUE WHERE id=:id", label=label, id=capture_id)
        con.close()
        print(f"[label] id={capture_id} → tile {label}")
    except Exception as e:
        print(f"[label] DB error: {e}")

@app.route('/health')
def health():
    return jsonify({'ok': True, 'time': datetime.utcnow().isoformat()})

@app.route('/upload', methods=['POST'])
def upload():
    captures = request.get_json(force=True).get('captures', [])
    if not captures: return jsonify({'error': 'no captures'}), 400
    inserted = 0
    con = get_db()
    for c in captures:
        variant  = c.get('variant', 'unknown')
        instr    = c.get('instruction', '')
        images   = c.get('images', [])
        combined = c.get('combined') or (images[0] if images else None)
        if not images: continue
        rows = con.run("""
            INSERT INTO captures (variant, instruction, images, combined, labeled, label)
            VALUES (:v, :i, :imgs, :comb, FALSE, NULL) RETURNING id
        """, v=variant, i=instr, imgs=json.dumps(images), comb=combined)
        capture_id = rows[0][0]
        inserted += 1
        if combined and NOPECHA_KEY:
            threading.Thread(target=label_worker, args=(capture_id, combined, instr), daemon=True).start()
    con.close()
    return jsonify({'ok': True, 'inserted': inserted})

@app.route('/dataset')
def dataset():
    variant = request.args.get('variant')
    limit   = min(int(request.args.get('limit', 5000)), 10000)
    con = get_db()
    if variant:
        rows = con.run("SELECT variant, instruction, images, label FROM captures WHERE labeled=TRUE AND variant=:v ORDER BY id DESC LIMIT :l", v=variant, l=limit)
    else:
        rows = con.run("SELECT variant, instruction, images, label FROM captures WHERE labeled=TRUE ORDER BY id DESC LIMIT :l", l=limit)
    con.close()
    result = [{'variant': r[0], 'instruction': r[1], 'images': json.loads(r[2]), 'label': r[3], 'labeled': True} for r in rows]
    return jsonify({'count': len(result), 'captures': result})

@app.route('/stats')
def stats():
    con = get_db()
    rows = con.run("SELECT variant, COUNT(*) as total, SUM(CASE WHEN labeled THEN 1 ELSE 0 END) as labeled FROM captures GROUP BY variant ORDER BY total DESC")
    con.close()
    data = [{'variant': r[0], 'total': r[1], 'labeled': r[2]} for r in rows]
    return jsonify({'variants': data, 'total': sum(r['total'] for r in data), 'labeled': sum(r['labeled'] for r in data)})

@app.route('/label', methods=['POST'])
def label_single():
    data = request.get_json(force=True)
    combined = data.get('combined')
    if not combined: return jsonify({'error': 'no image'}), 400
    label = nopecha_label(combined, data.get('instruction', ''))
    if label is None: return jsonify({'error': 'nopecha failed'}), 502
    return jsonify({'label': label})

@app.route('/relabel', methods=['POST'])
def relabel():
    if request.headers.get('X-Admin-Key') != ADMIN_KEY:
        return jsonify({'error': 'unauthorized'}), 401
    con = get_db()
    rows = con.run("SELECT id, combined, instruction FROM captures WHERE labeled=FALSE AND combined IS NOT NULL LIMIT 500")
    con.close()
    for r in rows:
        threading.Thread(target=label_worker, args=(r[0], r[1], r[2]), daemon=True).start()
    return jsonify({'ok': True, 'queued': len(rows)})

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
