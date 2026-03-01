"""
Captcha Crowdsource Server
Deploy free on Railway.app
"""

import os
import json
import time
import threading
import requests
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)

DATABASE_URL = os.environ.get("DATABASE_URL")
NOPECHA_KEY  = os.environ.get("NOPECHA_KEY")
ADMIN_KEY    = os.environ.get("ADMIN_KEY")

def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS captures (
                    id          SERIAL PRIMARY KEY,
                    variant     TEXT NOT NULL,
                    instruction TEXT,
                    images      JSONB NOT NULL,
                    combined    TEXT,
                    label       INTEGER,
                    labeled     BOOLEAN DEFAULT FALSE,
                    source      TEXT DEFAULT 'extension',
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_variant ON captures(variant);
                CREATE INDEX IF NOT EXISTS idx_labeled ON captures(labeled);
            """)
        conn.commit()
    print("DB initialized")

def nopecha_label(combined_b64, instruction):
    img = combined_b64.split(',')[1] if ',' in combined_b64 else combined_b64
    try:
        r = requests.post('https://api.nopecha.com/', json={
            'type': 'funcaptcha',
            'key': NOPECHA_KEY,
            'image_data': [img],
            'task': instruction or 'Pick the image'
        }, timeout=15)
        data = r.json()
        if data.get('error'):
            return None
        job_id = data.get('data')
        if not job_id:
            return None
        for _ in range(40):
            time.sleep(0.6)
            r2 = requests.get(f'https://api.nopecha.com/?key={NOPECHA_KEY}&id={job_id}', timeout=10)
            d2 = r2.json()
            if d2.get('error') == 14:
                continue
            if d2.get('error'):
                return None
            if isinstance(d2.get('data'), list):
                return next((i for i, v in enumerate(d2['data']) if v is True), None)
        return None
    except Exception as e:
        print(f"NopeCHA error: {e}")
        return None

def label_worker(capture_id, combined, instruction):
    label = nopecha_label(combined, instruction)
    if label is None:
        return
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE captures SET label=%s, labeled=TRUE WHERE id=%s", (label, capture_id))
            conn.commit()
        print(f"[label] id={capture_id} → tile {label}")
    except Exception as e:
        print(f"[label] DB error: {e}")


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'ok': True, 'time': datetime.utcnow().isoformat()})


@app.route('/upload', methods=['POST'])
def upload():
    data = request.get_json(force=True)
    captures = data.get('captures', [])
    if not captures:
        return jsonify({'error': 'no captures'}), 400

    inserted = 0
    with get_db() as conn:
        with conn.cursor() as cur:
            for c in captures:
                variant  = c.get('variant', 'unknown')
                instr    = c.get('instruction', '')
                images   = c.get('images', [])
                combined = c.get('combined') or (images[0] if images else None)
                if not images:
                    continue
                cur.execute("""
                    INSERT INTO captures (variant, instruction, images, combined, labeled, label)
                    VALUES (%s, %s, %s, %s, FALSE, NULL)
                    RETURNING id
                """, (variant, instr, json.dumps(images), combined))
                row = cur.fetchone()
                inserted += 1
                if combined and NOPECHA_KEY:
                    threading.Thread(target=label_worker, args=(row['id'], combined, instr), daemon=True).start()
        conn.commit()

    return jsonify({'ok': True, 'inserted': inserted})


@app.route('/dataset', methods=['GET'])
def dataset():
    variant = request.args.get('variant')
    limit   = min(int(request.args.get('limit', 5000)), 10000)
    with get_db() as conn:
        with conn.cursor() as cur:
            if variant:
                cur.execute("SELECT variant, instruction, images, label FROM captures WHERE labeled=TRUE AND variant=%s ORDER BY id DESC LIMIT %s", (variant, limit))
            else:
                cur.execute("SELECT variant, instruction, images, label FROM captures WHERE labeled=TRUE ORDER BY id DESC LIMIT %s", (limit,))
            rows = cur.fetchall()

    result = []
    for r in rows:
        imgs = r['images'] if isinstance(r['images'], list) else json.loads(r['images'])
        result.append({'variant': r['variant'], 'instruction': r['instruction'], 'images': imgs, 'label': r['label'], 'labeled': True})
    return jsonify({'count': len(result), 'captures': result})


@app.route('/stats', methods=['GET'])
def stats():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT variant, COUNT(*) as total,
                       SUM(CASE WHEN labeled THEN 1 ELSE 0 END) as labeled
                FROM captures GROUP BY variant ORDER BY total DESC
            """)
            rows = cur.fetchall()
    return jsonify({'variants': [dict(r) for r in rows], 'total': sum(r['total'] for r in rows), 'labeled': sum(r['labeled'] for r in rows)})


@app.route('/label', methods=['POST'])
def label_single():
    """Extension calls this to label one image — NopeCHA key stays server-side."""
    data = request.get_json(force=True)
    combined = data.get('combined')
    instruction = data.get('instruction', '')
    if not combined:
        return jsonify({'error': 'no image'}), 400
    label = nopecha_label(combined, instruction)
    if label is None:
        return jsonify({'error': 'nopecha failed'}), 502
    return jsonify({'label': label})


@app.route('/relabel', methods=['POST'])
def relabel():
    """Admin: re-run NopeCHA on all unlabeled captures."""
    if request.headers.get('X-Admin-Key') != ADMIN_KEY:
        return jsonify({'error': 'unauthorized'}), 401
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, combined, instruction FROM captures WHERE labeled=FALSE AND combined IS NOT NULL LIMIT 500")
            rows = cur.fetchall()
    for r in rows:
        threading.Thread(target=label_worker, args=(r['id'], r['combined'], r['instruction']), daemon=True).start()
    return jsonify({'ok': True, 'queued': len(rows)})


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
