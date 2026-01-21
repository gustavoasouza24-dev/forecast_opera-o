
# app.py — Previsão de Entregas (SARIMAX) multi-séries (CSV/Excel) + feriados em memória
import streamlit as st
import pandas as pd
import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
from io import BytesIO
from datetime import timedelta
import warnings
import zipfile

st.set_page_config(page_title="Previsão de Entregas (SARIMAX)", page_icon="📦", layout="wide")

# =========================
# Utilidades de leitura
# =========================
def sniff_csv_from_bytes(content: bytes):
    """Tenta ler um CSV de bytes com encodings/separadores comuns (pt-BR)."""
    last_err = None
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        for sep in (";", ",", "\t", "|"):
            try:
                bio = BytesIO(content)
                df = pd.read_csv(bio, sep=sep, encoding=enc, engine="python")
                if not df.empty and not all(str(c).startswith("Unnamed") for c in df.columns):
                    return df, sep, enc
            except Exception as e:
                last_err = e
    raise RuntimeError(f"Falha ao ler CSV. Último erro: {last_err}")

def read_excel_bytes(content: bytes, ext: str, sheet_name: str | None = None) -> tuple[pd.DataFrame, list[str]]:
    """Lê Excel de bytes. Se sheet_name=None, retorna apenas a lista de abas."""
    engine = "openpyxl" if ext == ".xlsx" else "xlrd"
    xbio = BytesIO(content)
    xls = pd.ExcelFile(xbio, engine=engine)
    if sheet_name is None:
        return None, xls.sheet_names
    xbio2 = BytesIO(content)
    df = pd.read_excel(xbio2, sheet_name=sheet_name, engine=engine)
    return df, xls.sheet_names

# =========================
# Utilidades gerais
# =========================
def to_datetime_col(s: pd.Series):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d/%m/%y"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce", dayfirst=True)

def clean_numeric_col(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

# =========================
# Feriados em memória (qualquer ano)
# =========================
def easter_sunday(year: int) -> pd.Timestamp:
    a = year % 19; b = year // 100; c = year % 100
    d = b // 4; e = b % 4; f = (b + 8) // 25; g = (b - f + 1) // 3
    h = (19*a + b - d - g + 15) % 30
    i = c // 4; k = c % 4
    L = (32 + 2*e + 2*i - h - k) % 7
    m = (a + 11*h + 22*L) // 451
    month = (h + L - 7*m + 114) // 31
    day = ((h + L - 7*m + 114) % 31) + 1
    return pd.Timestamp(year=year, month=month, day=day)

def make_br_holiday_set(year: int, include_facultativos: bool, include_joinville: bool) -> set[str]:
    S = {
        f"{year}-01-01", f"{year}-04-21", f"{year}-05-01",
        f"{year}-09-07", f"{year}-10-12", f"{year}-11-02", f"{year}-11-15",
    }
    # Consciência Negra (nacional desde 2023)
    if year >= 2023:
        S.add(f"{year}-11-20")
    pascoa = easter_sunday(year)
    # Sexta-Feira Santa
    S.add((pascoa - pd.Timedelta(days=2)).strftime("%Y-%m-%d"))
    # Pontos facultativos (opcional)
    if include_facultativos:
        S.update({
            (pascoa - pd.Timedelta(days=48)).strftime("%Y-%m-%d"),  # Carnaval (seg)
            (pascoa - pd.Timedelta(days=47)).strftime("%Y-%m-%d"),  # Carnaval (ter)
            (pascoa - pd.Timedelta(days=46)).strftime("%Y-%m-%d"),  # Cinzas
            (pascoa + pd.Timedelta(days=60)).strftime("%Y-%m-%d"),  # Corpus Christi
        })
    # Joinville/SC (municipal)
    if include_joinville:
        S.add(f"{year}-03-09")
    return S

def holidays_memory(index: pd.DatetimeIndex, include_facultativos: bool, include_joinville: bool) -> pd.Series:
    years = sorted(set(index.year))
    S = set()
    for y in years:
        S |= make_br_holiday_set(y, include_facultativos, include_joinville)
    return index.to_series().apply(lambda d: 1 if d.strftime("%Y-%m-%d") in S else 0).rename("is_holiday")

# =========================
# Sidebar — parâmetros
# =========================
st.sidebar.header("Parâmetros SARIMAX")
col_ns1, col_ns2, col_ns3 = st.sidebar.columns(3)
p = col_ns1.number_input("p", min_value=0, value=1, step=1)
d = col_ns2.number_input("d", min_value=0, value=1, step=1)
q = col_ns3.number_input("q", min_value=0, value=1, step=1)

col_s1, col_s2, col_s3, col_s4 = st.sidebar.columns(4)
P = col_s1.number_input("P", min_value=0, value=1, step=1)
D = col_s2.number_input("D", min_value=0, value=1, step=1)
Q = col_s3.number_input("Q", min_value=0, value=1, step=1)
s = col_s4.number_input("Período s", min_value=2, value=7, step=1)

st.sidebar.markdown("---")
holiday_source = st.sidebar.selectbox(
    "Feriados",
    options=["Em memória (BR)", "Upload CSV", "Sem feriados"],
    index=0
)
use_weekend = st.sidebar.checkbox("Usar final de semana (is_weekend)", value=True)
include_facultativos = st.sidebar.checkbox("Incluir facultativos (Carnaval/Corpus)", value=True)
include_joinville = st.sidebar.checkbox("Incluir feriados de Joinville/SC", value=False)

st.sidebar.markdown("---")
do_backtest = st.sidebar.checkbox("Backtest (últimos N dias)", value=False)
bt_days = st.sidebar.number_input("N dias de backtest", min_value=7, max_value=90, value=30, step=1)

# =========================
# Upload CSV/Excel e mapeamento
# =========================
st.title("Previsão SARIMAX - FMA Qualificação")
st.write("Envie um **CSV** ou **Excel** com **uma coluna de data** e **várias colunas numéricas** de entregas.")

up = st.file_uploader("📥 Envie CSV/Excel", type=["csv", "xlsx", "xls"])
if not up:
    st.stop()

name = (up.name or "").lower()
content = up.read()

# 1) Excel?
if name.endswith(".xlsx") or name.endswith(".xls"):
    ext = ".xlsx" if name.endswith(".xlsx") else ".xls"
    try:
        _, sheet_names = read_excel_bytes(content, ext, sheet_name=None)
    except Exception as e:
        st.error(f"Erro ao abrir Excel: {e}"); st.stop()
    sheet = st.selectbox("Selecione a planilha", options=sheet_names, index=0)
    try:
        raw_df, _ = read_excel_bytes(content, ext, sheet_name=sheet)
    except Exception as e:
        st.error(f"Erro ao ler a planilha '{sheet}': {e}"); st.stop()
    used_sep, used_enc = "(excel)", "(excel)"

# 2) CSV?
elif name.endswith(".csv"):
    try:
        raw_df, used_sep, used_enc = sniff_csv_from_bytes(content)
    except Exception as e:
        st.error(f"Erro ao ler o CSV: {e}"); st.stop()
else:
    st.error("Formato não suportado. Envie .csv, .xlsx ou .xls."); st.stop()

raw_df.columns = [str(c).strip() for c in raw_df.columns]
with st.expander("👀 Prévia do arquivo lido"):
    st.caption(f"Origem: **{name}** • Sep/Enc: `{used_sep}` / `{used_enc}`")
    st.dataframe(raw_df.head(10), use_container_width=True)

# Detecta coluna de data
date_candidates = []
for c in raw_df.columns:
    parsed = to_datetime_col(raw_df[c])
    if parsed.notna().mean() > 0.8:
        date_candidates.append(c)
default_date_col = None
for cand in ("data", "date", "dt"):
    if cand in [c.lower() for c in raw_df.columns]:
        default_date_col = next(col for col in raw_df.columns if col.lower() == cand); break
if default_date_col is None and date_candidates:
    default_date_col = date_candidates[0]

date_col = st.selectbox("Selecione a coluna de **data**", options=raw_df.columns.tolist(),
                        index=(raw_df.columns.tolist().index(default_date_col) if default_date_col in raw_df.columns else 0))
dt_series = to_datetime_col(raw_df[date_col])
if dt_series.isna().all():
    st.error(f"A coluna '{date_col}' não pôde ser convertida em datas."); st.stop()

# Coerção numérica para todas as demais (multi-séries)
num_df = raw_df.drop(columns=[date_col], errors="ignore").copy()
for c in num_df.columns:
    num_df[c] = clean_numeric_col(num_df[c])
numeric_cols = [c for c in num_df.columns if pd.api.types.is_numeric_dtype(num_df[c])]
if not numeric_cols:
    st.error("Não encontrei colunas numéricas para previsão. Confira o arquivo."); st.stop()

select_all = st.checkbox("Selecionar todas as colunas numéricas", value=True)
default_targets = numeric_cols if select_all else numeric_cols[:1]
targets = st.multiselect("Selecione as **colunas de entregas** (múltiplas):",
                         options=numeric_cols, default=default_targets)
if not targets:
    st.warning("Selecione ao menos uma coluna para prever."); st.stop()

# Base diária com TODAS as colunas selecionadas
df = pd.DataFrame({"data": dt_series}).join(num_df[targets])
df = df.dropna(subset=["data"]).sort_values("data").set_index("data").asfreq("D")
df = df.apply(lambda s: s.fillna(method="ffill").fillna(0))  # ffill -> 0, por coluna

# =========================
# Janela de previsão
# =========================
last_date = df.index.max()
st.info(f"📅 Histórico até **{last_date.date()}**. Treino até essa data; previsão inicia em **{(last_date + pd.Timedelta(days=1)).date()}**.")
forecast_start = last_date + pd.Timedelta(days=1)
default_end = forecast_start + pd.Timedelta(days=30)
forecast_end = st.date_input("Data final da previsão", value=default_end.date(), min_value=forecast_start.date())
forecast_end = pd.to_datetime(forecast_end)
forecast_index = pd.date_range(start=forecast_start, end=forecast_end, freq="D")

# =========================
# Exógenas (finais de semana + feriados) — **CORRIGIDO**
# =========================
if use_weekend:
    # ✅ Garante um pd.Series com índice e nome (evita AttributeError ao chamar .rename)
    is_weekend_tr = pd.Series(
        (df.index.dayofweek >= 5).astype(int),
        index=df.index,
        name="is_weekend"
    )
    is_weekend_fc = pd.Series(
        (forecast_index.dayofweek >= 5).astype(int),
        index=forecast_index,
        name="is_weekend"
    )
else:
    is_weekend_tr = pd.Series(0, index=df.index, name="is_weekend")
    is_weekend_fc = pd.Series(0, index=forecast_index, name="is_weekend")

# Feriados
if holiday_source == "Em memória (BR)":
    is_holiday_train = holidays_memory(df.index, include_facultativos, include_joinville)  # já vem .rename("is_holiday")
    is_holiday_fc = holidays_memory(forecast_index, include_facultativos, include_joinville)
elif holiday_source == "Upload CSV":
    st.info("Envie um CSV com coluna 'data' (YYYY-MM-DD) marcando feriados.")
    up_h = st.file_uploader("CSV de feriados", type=["csv"], key="hcsv")
    if up_h:
        try:
            hdf = pd.read_csv(up_h)
            H = set(pd.to_datetime(hdf["data"]).dt.strftime("%Y-%m-%d").tolist())
            is_holiday_train = df.index.to_series().apply(lambda d: 1 if d.strftime("%Y-%m-%d") in H else 0).rename("is_holiday")
            is_holiday_fc = forecast_index.to_series().apply(lambda d: 1 if d.strftime("%Y-%m-%d") in H else 0).rename("is_holiday")
        except Exception as e:
            st.error(f"Erro no CSV de feriados: {e}")
            is_holiday_train = pd.Series(0, index=df.index, name="is_holiday")
            is_holiday_fc = pd.Series(0, index=forecast_index, name="is_holiday")
    else:
        is_holiday_train = pd.Series(0, index=df.index, name="is_holiday")
        is_holiday_fc = pd.Series(0, index=forecast_index, name="is_holiday")
else:
    is_holiday_train = pd.Series(0, index=df.index, name="is_holiday")
    is_holiday_fc = pd.Series(0, index=forecast_index, name="is_holiday")

# ✅ Séries já estão nomeadas; concatenação direta
exog_train_base = pd.concat([is_weekend_tr, is_holiday_train], axis=1)
exog_fc_base    = pd.concat([is_weekend_fc,  is_holiday_fc],  axis=1)

with st.expander("🔍 Auditoria de feriados na previsão"):
    audit = pd.DataFrame({
        "data": forecast_index.date,
        "is_weekend": is_weekend_fc.values,
        "is_holiday": is_holiday_fc.values
    }).set_index("data")
    st.dataframe(audit, use_container_width=True)

# =========================
# Funções de modelagem
# =========================
def prepare_series(full_index_end: pd.Timestamp, s: pd.Series) -> pd.Series:
    full_idx = pd.date_range(start=s.index.min(), end=full_index_end, freq="D")
    out = s.reindex(full_idx)
    out = out.fillna(method="ffill").fillna(0)
    return out

def fit_and_forecast(series, exog_train, exog_fc, order, seasonal_order):
    model = sm.tsa.statespace.SARIMAX(
        series, exog=exog_train, order=order, seasonal_order=seasonal_order,
        enforce_stationarity=False, enforce_invertibility=False
    )
    res = model.fit(disp=False, maxiter=100)
    pred_obj = res.get_forecast(steps=len(exog_fc), exog=exog_fc)
    pred_mean = pred_obj.predicted_mean.clip(lower=0)
    ci = pred_obj.conf_int()
    ci_lower = ci.iloc[:, 0].clip(lower=0)
    ci_upper = ci.iloc[:, 1].clip(lower=0)
    return pred_mean, ci_lower, ci_upper

# =========================
# Backtest (opcional) — multi-séries
# =========================
if do_backtest and st.button("📏 Rodar Backtest"):
    st.subheader("🧪 Backtest (últimos N dias)")
    bt_end = last_date
    bt_start = last_date - pd.Timedelta(days=bt_days) + pd.Timedelta(days=1)
    if bt_start <= df.index.min():
        st.warning("Janela de backtest muito longa para o histórico disponível. Reduza N dias.")
    else:
        bt_index = pd.date_range(start=bt_start, end=bt_end, freq="D")
        metrics = []
        for target in targets:
            st.markdown(f"**Série:** `{target}`")
            train_end = bt_start - pd.Timedelta(days=1)
            series_train = prepare_series(train_end, df[target])
            exog_tr = exog_train_base.reindex(series_train.index)
            exog_bt = pd.concat([
                pd.Series((bt_index.dayofweek >= 5).astype(int), index=bt_index, name="is_weekend") if use_weekend else pd.Series(0, index=bt_index, name="is_weekend"),
                holidays_memory(bt_index, include_facultativos, include_joinville) if holiday_source == "Em memória (BR)" else
                is_holiday_train.reindex(bt_index).rename("is_holiday").fillna(0)
            ], axis=1)

            try:
                mean_bt, li_bt, ls_bt = fit_and_forecast(series_train, exog_tr, exog_bt,
                                                         order=(p, d, q), seasonal_order=(P, D, Q, s))
            except Exception as e:
                st.error(f"Falha no backtest de '{target}': {e}")
                continue

            real_bt = df[target].reindex(bt_index).fillna(0)
            mape = (np.abs((real_bt - mean_bt) / np.where(real_bt==0, np.nan, real_bt))).mean() * 100
            rmse = np.sqrt(((real_bt - mean_bt) ** 2).mean())
            metrics.append({"série": target, "MAPE (%)": round(mape,2), "RMSE": round(rmse,2)})

            fig, ax = plt.subplots(figsize=(10,5))
            real_bt.plot(ax=ax, label="Real (backtest)", color="C0")
            mean_bt.plot(ax=ax, label="Previsto", color="C3", linestyle="--")
            ax.fill_between(li_bt.index, li_bt, ls_bt, color="C3", alpha=0.2, label="IC 95%")
            ax.set_title(f"Backtest — {target} ({bt_days} dias)")
            ax.grid(True); ax.legend()
            st.pyplot(fig)

        if metrics:
            st.markdown("**Métricas por série:**")
            st.dataframe(pd.DataFrame(metrics), use_container_width=True)

# =========================
# Previsão — multi-séries
# =========================
if st.button("🚀 Gerar Previsão", type="primary"):
    all_forecasts = []
    plot_imgs = {}
    progress = st.progress(0)
    total = len(targets)

    for i, target in enumerate(targets, start=1):
        st.subheader(f"🔧 Série: **{target}**")
        series_train = prepare_series(last_date, df[target])
        exog_tr = exog_train_base.reindex(series_train.index)

        warnings.filterwarnings("ignore")
        try:
            pred_mean, ci_lower, ci_upper = fit_and_forecast(
                series_train, exog_tr, exog_fc_base,
                order=(p, d, q), seasonal_order=(P, D, Q, s)
            )
        except Exception as e:
            warnings.filterwarnings("default")
            st.error(f"Falha ao ajustar/prever '{target}': {e}")
            progress.progress(i/total)
            continue
        warnings.filterwarnings("default")

        # Tabela p/ export (uma por série)
        out_df = pd.DataFrame({
            f"{target}_Previsao": pred_mean.round().astype(int),
            f"{target}_LI_95%": ci_lower.round().astype(int),
            f"{target}_LS_95%": ci_upper.round().astype(int)
        }, index=forecast_index)
        all_forecasts.append(out_df)

        # Gráfico
        fig, ax = plt.subplots(figsize=(12,6))
        series_train.tail(90).plot(ax=ax, label="Treino (últimos 90d)")
        pred_mean.plot(ax=ax, label="Previsão", color="red", linestyle="--")
        ax.fill_between(ci_lower.index, ci_lower, ci_upper, color="red", alpha=0.2, label="IC 95%")
        real_data = df[target].reindex(forecast_index)
        if real_data.notna().any():
            real_data.plot(ax=ax, label="Real (período previsto)", color="orange", linewidth=2)
        ax.set_title(f"SARIMAX — {target}")
        ax.grid(True); ax.legend()
        buf = BytesIO(); plt.tight_layout(); plt.savefig(buf, format="png", dpi=150); plt.close(fig)
        plot_imgs[target] = buf.getvalue()
        st.image(plot_imgs[target], caption=f"Gráfico — {target}", use_column_width=True)

        progress.progress(i/total)

    # Saídas combinadas
    if not all_forecasts:
        st.warning("Nenhuma previsão gerada."); st.stop()

    combined = pd.concat(all_forecasts, axis=1)
    st.subheader("📄 Tabela combinada (todas as séries selecionadas)")
    st.dataframe(combined, use_container_width=True)

    # Download CSV combinado
    csv_bytes = combined.to_csv(sep=";", decimal=",", index_label="data").encode("utf-8-sig")
    st.download_button(
        "💾 Baixar CSV combinado",
        data=csv_bytes,
        file_name="previsao_multiseries.csv",
        mime="text/csv",
        use_container_width=True
    )

    # ZIP de gráficos
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for col, img in plot_imgs.items():
            zf.writestr(f"sarimax_forecast_{col}.png", img)
    st.download_button(
        "🗜️ Baixar gráficos (.zip)",
        data=zip_buf.getvalue(),
        file_name="sarimax_graficos_multiseries.zip",
        mime="application/zip",
        use_container_width=True
    )
