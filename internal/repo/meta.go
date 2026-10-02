package repo

import (
	"database/sql"
	"errors"
	"strconv"
)

// GetMeta reads one repository setting.
func (m *Manifest) GetMeta(key string) (string, bool, error) {
	var v string
	err := m.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// PutMeta writes one repository setting.
func (m *Manifest) PutMeta(key, value string) error {
	_, err := m.db.Exec(`INSERT INTO meta(key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// MetaPolynomialKey stores the Rabin polynomial as a decimal uint64.
const MetaPolynomialKey = "chunker_polynomial"

// GetPolynomial returns the stored polynomial; ok=false on first use.
func (m *Manifest) GetPolynomial() (uint64, bool, error) {
	v, ok, err := m.GetMeta(MetaPolynomialKey)
	if err != nil || !ok {
		return 0, ok, err
	}
	n, err := strconv.ParseUint(v, 10, 64)
	return n, err == nil, err
}

// SetPolynomial persists the chunking polynomial.
func (m *Manifest) SetPolynomial(p uint64) error {
	return m.PutMeta(MetaPolynomialKey, strconv.FormatUint(p, 10))
}
