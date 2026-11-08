// HyperData screener signals on a TigerTrade chart.
//
// Polls the local screener (GET /v1/scalper/chart-marks) for the chart's own instrument and draws:
//  - a compact marker per card: ▲ from below (buyers), ▼ from above (sellers), ◆ on the price (no side);
//    bigger for strong, fading with age;
//  - the impulse leg that led to the card and two 15s bars (buys green, sells red) beside the marker;
//  - the outcome, measured on the chart's own bars: price after up to 5 minutes with a badge, and the
//    band of the best/worst move over that time;
//  - the zone as a box from when it appeared, thickness by its $ size, "eaten in ~Ns";
//  - a collapsed panel (full card on hover), the full card next to the cursor on a marker's bar,
//    and an optional Tiger alert (sound/popup) when a new card arrives for this instrument.
// Read-only: it never trades and never changes the screener.

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Net.Http;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using TigerTrade.Chart.Alerts;
using TigerTrade.Chart.Base;
using TigerTrade.Chart.Indicators.Common;
using TigerTrade.Chart.Indicators.Enums;
using TigerTrade.Core.Utils.Time;
using TigerTrade.Dx;
using TigerTrade.Dx.Enums;

namespace TigerTrade.Chart.Indicators.Custom
{
    [DataContract(Name = "HyperDataSignalsIndicator", Namespace = "http://schemas.datacontract.org/2004/07/TigerTrade.Chart.Indicators.Custom")]
    [Indicator("X_HyperDataSignals", "*HyperData Signals", true, Type = typeof(HyperDataSignalsIndicator))]
    internal sealed class HyperDataSignalsIndicator : IndicatorBase
    {
        private static readonly HttpClient Http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };

        // No field initializers: TigerTrade restores indicators with DataContract deserialization, which
        // skips constructors and initializers. SetDefaults runs from both the constructor and OnDeserializing.
        private string _url;
        private int _minutes;
        private int _textSize;
        private bool _showPanel;
        private bool _showEmoji;
        private bool _showOutcome;
        private bool _showImpulse;
        private ChartAlertSettings _alert;

        [DataMember(Name = "Url"), DefaultValue("http://127.0.0.1:8421")]
        [Category("Скринер"), DisplayName("Адрес скринера")]
        public string Url
        {
            get => _url;
            set { _url = string.IsNullOrWhiteSpace(value) ? "http://127.0.0.1:8421" : value.Trim().TrimEnd('/'); OnPropertyChanged(nameof(Url)); }
        }

        [DataMember(Name = "Minutes"), DefaultValue(120)]
        [Category("Скринер"), DisplayName("История, минут")]
        public int Minutes
        {
            get => _minutes;
            set { _minutes = Math.Max(5, Math.Min(value, 1440)); OnPropertyChanged(nameof(Minutes)); }
        }

        [DataMember(Name = "TextSize"), DefaultValue(11)]
        [Category("Вид"), DisplayName("Размер текста")]
        public int TextSize
        {
            get => _textSize;
            set { _textSize = Math.Max(8, Math.Min(value, 24)); OnPropertyChanged(nameof(TextSize)); }
        }

        [DataMember(Name = "ShowPanel"), DefaultValue(true)]
        [Category("Вид"), DisplayName("Панель (полная при наведении)")]
        public bool ShowPanel
        {
            get => _showPanel;
            set { _showPanel = value; OnPropertyChanged(nameof(ShowPanel)); }
        }

        [DataMember(Name = "ShowOutcome"), DefaultValue(true)]
        [Category("Вид"), DisplayName("Результат сигнала (до 5 минут)")]
        public bool ShowOutcome
        {
            get => _showOutcome;
            set { _showOutcome = value; OnPropertyChanged(nameof(ShowOutcome)); }
        }

        [DataMember(Name = "ShowImpulse"), DefaultValue(true)]
        [Category("Вид"), DisplayName("Импульс и объёмы 15с")]
        public bool ShowImpulse
        {
            get => _showImpulse;
            set { _showImpulse = value; OnPropertyChanged(nameof(ShowImpulse)); }
        }

        [DataMember(Name = "ShowEmoji"), DefaultValue(false)]
        [Category("Вид"), DisplayName("Эмодзи в тексте")]
        public bool ShowEmoji
        {
            get => _showEmoji;
            set { _showEmoji = value; OnPropertyChanged(nameof(ShowEmoji)); }
        }

        [DataMember(Name = "Alert")]
        [Category("Алерт"), DisplayName("Новый сигнал")]
        public ChartAlertSettings Alert
        {
            get => _alert ?? (_alert = new ChartAlertSettings());
            set { if (Equals(value, _alert)) return; _alert = value; OnPropertyChanged(nameof(Alert)); }
        }

        public override bool ShowIndicatorValues => false;
        public override bool ShowIndicatorLabels => false;
        public override IndicatorCalculation Calculation => IndicatorCalculation.OnEachTick;

        private sealed class Mark
        {
            public long At;
            public double? Price;
            public int Side;
            public string Strength;
            public int? Tier;
            public List<string> Lines = new List<string>();
            public string ZoneSide;
            public double? ZonePrice;
            public double? ZoneUsd;
            public long? ZoneFirstSeen;
            public double? ZoneEatSeconds;
            public double? Buys;
            public double? Sells;
            public double? RecentMoveBps;
            public double? PriorMoveBps;
        }

        private sealed class Snapshot
        {
            public List<Mark> Marks = new List<Mark>();
            public Mark Live;
            public string Summary;
        }

        private sealed class Placed
        {
            public int Index;
            public Mark Mark;
        }

        private object _lock;
        private Snapshot _data;
        private string _error;
        private int _version;
        private int _drawnVersion;
        private string _code;
        private string _exchange;
        private CancellationTokenSource _poll;
        private long _alertedAt;
        private List<Placed> _placed;
        private Rect _panelRect;
        private List<string> _panelFull;

        public HyperDataSignalsIndicator()
        {
            SetDefaults();
            ShowIndicatorTitle = false;
        }

        [OnDeserializing]
        private void OnDeserializing(StreamingContext context)
        {
            SetDefaults();
        }

        private void SetDefaults()
        {
            _url = "http://127.0.0.1:8421";
            _minutes = 120;
            _textSize = 11;
            _showPanel = true;
            _showEmoji = false;
            _showOutcome = true;
            _showImpulse = true;
            _lock = new object();
            _drawnVersion = -1;
            _code = "";
            _exchange = "";
            _alertedAt = -1;
            _placed = new List<Placed>();
            _panelRect = Rect.Empty;
            _panelFull = new List<string>();
        }

        // ---------------------------------------------------------------- data

        protected override void Execute()
        {
            var symbol = DataProvider?.Symbol;
            if (symbol == null)
                return;
            var code = symbol.Code ?? symbol.Name ?? "";
            if (code != _code || (symbol.Exchange ?? "") != _exchange)
            {
                _code = code;
                _exchange = symbol.Exchange ?? "";
                lock (_lock) { _data = null; _error = null; }
                _alertedAt = -1;
                Interlocked.Increment(ref _version);
            }
            if (_poll == null)
            {
                _poll = new CancellationTokenSource();
                var token = _poll.Token;
                Task.Run(() => PollLoop(token));
            }
        }

        private async Task PollLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var query = $"{_url}/v1/scalper/chart-marks?code={Uri.EscapeDataString(_code)}" +
                                $"&exchange={Uri.EscapeDataString(_exchange)}&minutes={_minutes}";
                    var body = await Http.GetStringAsync(query, token).ConfigureAwait(false);
                    var parsed = Parse(body);
                    lock (_lock) { _data = parsed; _error = null; }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception e)
                {
                    lock (_lock) { _error = e is HttpRequestException || e is TaskCanceledException ? "скринер недоступен" : "ошибка данных"; }
                }
                Interlocked.Increment(ref _version);
                try { await Task.Delay(2000, token).ConfigureAwait(false); } catch (OperationCanceledException) { return; }
            }
        }

        private static Snapshot Parse(string body)
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            var snapshot = new Snapshot();
            if (root.TryGetProperty("marks", out var marks) && marks.ValueKind == JsonValueKind.Array)
                foreach (var m in marks.EnumerateArray())
                    snapshot.Marks.Add(ParseMark(m));
            if (root.TryGetProperty("live", out var live) && live.ValueKind == JsonValueKind.Object)
                snapshot.Live = ParseMark(live);
            if (root.TryGetProperty("summary", out var summary) && summary.ValueKind == JsonValueKind.Object)
                snapshot.Summary = SummaryLine(summary);
            return snapshot;
        }

        // "За 24ч: 5 сигналов · по сторонам 3 из 4 в плюс через 5м · в пользу 0.40% / против 0.25%"
        private static string SummaryLine(JsonElement s)
        {
            var sent = (int)(Number(s, "sent") ?? 0);
            if (sent == 0)
                return null;
            var hours = (int)(Number(s, "hours") ?? 24);
            var sided = (int)(Number(s, "sided") ?? 0);
            var right = (int)(Number(s, "right") ?? 0);
            var minutes = (int)(Number(s, "horizon_minutes") ?? 5);
            var pending = (int)(Number(s, "pending") ?? 0);
            var line = $"За {hours}ч: сигналов {sent}";
            if (sided > 0)
            {
                line += $" · со стороной {right} из {sided} в плюс через {minutes}м";
                var favour = Number(s, "favour_pct");
                var against = Number(s, "against_pct");
                if (favour.HasValue && against.HasValue)
                    line += $" · ср. в пользу {favour:0.00}% / против {against:0.00}%";
            }
            if (pending > 0)
                line += $" · ждут оценки {pending}";
            var unscored = (int)(Number(s, "unscored") ?? 0);
            if (unscored > 0)
                line += $" · без оценки {unscored}";
            return line;
        }

        private static double? Number(JsonElement e, string name) =>
            e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetDouble() : (double?)null;

        private static string Str(JsonElement e, string name) =>
            e.ValueKind == JsonValueKind.Object && e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

        private static Mark ParseMark(JsonElement e)
        {
            var mark = new Mark
            {
                At = (long)(Number(e, "at") ?? 0),
                Price = Number(e, "price"),
                Side = (int)(Number(e, "side") ?? 0),
                Strength = Str(e, "strength"),
                Tier = (int?)Number(e, "tier"),
            };
            if (e.TryGetProperty("lines", out var lines) && lines.ValueKind == JsonValueKind.Array)
                foreach (var line in lines.EnumerateArray())
                    if (line.ValueKind == JsonValueKind.String)
                        mark.Lines.Add(line.GetString());
            if (e.TryGetProperty("zone", out var zone) && zone.ValueKind == JsonValueKind.Object)
            {
                mark.ZoneSide = Str(zone, "side");
                mark.ZonePrice = Number(zone, "price");
                mark.ZoneUsd = Number(zone, "notional_usd");
                var first = Number(zone, "first_seen");
                mark.ZoneFirstSeen = first.HasValue ? (long)first.Value : (long?)null;
                mark.ZoneEatSeconds = Number(zone, "eat_seconds");
            }
            if (e.TryGetProperty("tape", out var tape) && tape.ValueKind == JsonValueKind.Object)
            {
                mark.Buys = Number(tape, "buys_usd");
                mark.Sells = Number(tape, "sells_usd");
                mark.RecentMoveBps = Number(tape, "recent_move_bps");
                mark.PriorMoveBps = Number(tape, "prior_move_bps");
            }
            return mark;
        }

        public override bool CheckNeedRedraw()
        {
            return Volatile.Read(ref _version) != _drawnVersion;
        }

        // ---------------------------------------------------------------- time and bars

        // Bar times are in the exchange clock; marks come in UTC. TimeHelper is what Tiger's own bar timer uses.
        private TimeSpan ExchangeOffset()
        {
            var exchange = DataProvider?.Symbol?.Exchange;
            if (string.IsNullOrEmpty(exchange))
                return TimeSpan.Zero;
            var offset = TimeHelper.GetCurrTime(exchange) - TimeHelper.UtcTime;
            return TimeSpan.FromMinutes(Math.Round(offset.TotalMinutes));
        }

        private static DateTime ChartTime(long unixMs, TimeSpan offset) =>
            DateTimeOffset.FromUnixTimeMilliseconds(unixMs).UtcDateTime + offset;

        // Data index of the bar that contains the time: the last bar opened at or before it, or -1.
        private int BarIndex(DateTime time)
        {
            var count = DataProvider?.Count ?? 0;
            if (count == 0 || DataProvider.GetCluster(0).Time > time)
                return -1;
            int lo = 0, hi = count - 1;
            while (lo < hi)
            {
                var mid = (lo + hi + 1) / 2;
                if (DataProvider.GetCluster(mid).Time <= time) lo = mid; else hi = mid - 1;
            }
            return lo;
        }

        // ---------------------------------------------------------------- text helpers

        private string Text(string line)
        {
            if (_showEmoji || string.IsNullOrEmpty(line))
                return line;
            var sb = new StringBuilder(line.Length);
            foreach (var c in line)
            {
                if (char.IsSurrogate(c) || c == '️' || c == '‍' || (c >= '☀' && c <= '➿') || (c >= '⬀' && c <= '⯿') || c == '↩')
                    continue;
                sb.Append(c);
            }
            return sb.ToString().Replace("  ", " ").Trim();
        }

        private static string Short(double usd)
        {
            if (usd >= 1e6) return (usd / 1e6).ToString("0.#") + "M";
            if (usd >= 1e3) return (usd / 1e3).ToString("0.#") + "k";
            return usd.ToString("0");
        }

        private static string Pct(double fraction) => (fraction >= 0 ? "+" : "") + (fraction * 100).ToString("0.00") + "%";

        // ---------------------------------------------------------------- colours

        private static XBrush Brush(byte a, byte r, byte g, byte b) => new XBrush(new XColor(a, r, g, b));
        private static XBrush Buy(byte a) => Brush(a, 38, 166, 91);
        private static XBrush Sell(byte a) => Brush(a, 235, 77, 75);
        private static XBrush Neutral(byte a) => Brush(a, 170, 170, 170);
        private static XBrush SideBrush(int side, byte a) => side > 0 ? Buy(a) : side < 0 ? Sell(a) : Neutral(a);

        // Fresh cards are bright; by the end of the history window they keep about a third of their strength.
        private byte Alpha(long at)
        {
            var ageMinutes = (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - at) / 60000.0;
            var fade = Math.Max(0, Math.Min(1, ageMinutes / Math.Max(10, _minutes)));
            return (byte)(255 - fade * 165);
        }

        // ---------------------------------------------------------------- render

        public override void Render(DxVisualQueue visual)
        {
            _drawnVersion = Volatile.Read(ref _version);
            Snapshot data;
            string error;
            lock (_lock) { data = _data; error = _error; }

            var font = new XFont(Canvas.ChartFont.Name, _textSize);
            var small = new XFont(Canvas.ChartFont.Name, Math.Max(8, _textSize - 2));
            var placed = new List<Placed>();
            var outcomeLabels = new List<Rect>();  // outcome captions already drawn this frame

            if (data != null)
            {
                RaiseAlert(data);
                var offset = ExchangeOffset();
                var first = Canvas.Stop;  // Tiger's visible data range is [Stop, Stop + Count]
                var last = Canvas.Stop + Canvas.Count;
                Mark latestZone = data.Live?.ZonePrice != null ? data.Live : null;
                if (latestZone == null)
                    foreach (var mark in data.Marks)
                        if (mark.ZonePrice != null && (latestZone == null || mark.At > latestZone.At))
                            latestZone = mark;

                foreach (var mark in data.Marks)
                {
                    if (mark.Price == null || mark.At == 0)
                        continue;
                    var index = BarIndex(ChartTime(mark.At, offset));
                    if (index < 0 || index > last)
                        continue;
                    var alpha = Alpha(mark.At);
                    if (mark.ZonePrice != null && mark != latestZone)
                        DrawZone(visual, mark, offset, false, small, (byte)(alpha / 2));
                    if (_showOutcome)
                        DrawOutcome(visual, mark, index, offset, small, alpha, outcomeLabels);
                    if (index < first)
                        continue;
                    if (_showImpulse)
                        DrawImpulse(visual, mark, index, offset, alpha);
                    DrawMarker(visual, mark, index, font, alpha);
                    placed.Add(new Placed { Index = index, Mark = mark });
                }
                if (latestZone?.ZonePrice != null)
                    DrawZone(visual, latestZone, offset, true, small, 255);
            }
            _placed = placed;
            DrawPanel(visual, data, error, font, placed.Count);
        }

        private void DrawMarker(DxVisualQueue visual, Mark mark, int index, XFont font, byte alpha)
        {
            var x = GetX(index);
            var y = GetY(mark.Price.Value);
            var brush = SideBrush(mark.Side, alpha);
            var size = mark.Strength == "strong" ? 8.0 : 6.0;
            double labelY;
            if (mark.Side == 0)
            {
                visual.FillPolygon(brush, new[] { new Point(x, y - size), new Point(x + size, y), new Point(x, y + size), new Point(x - size, y) });
                labelY = y - size - _textSize * 1.6;
            }
            else
            {
                var baseY = mark.Side > 0 ? y + size * 1.4 : y - size * 1.4;
                visual.FillPolygon(brush, new[] { new Point(x, y), new Point(x - size, baseY), new Point(x + size, baseY) });
                labelY = mark.Side > 0 ? baseY + 2 : baseY - _textSize * 1.8;
            }
            var verdict = Text(mark.Lines.Count > 1 ? mark.Lines[1] : mark.Lines.Count > 0 ? mark.Lines[0] : "");
            var prefix = (mark.Strength == "strong" ? "!! " : "") + (mark.Tier.HasValue ? $"T{mark.Tier} " : "");
            visual.DrawString(prefix + verdict, font, brush, x + size + 3, labelY, XTextAlignment.Left);
        }

        // Impulse leg (start of the 30s impulse → signal) and two 15s volume bars beside the marker.
        private void DrawImpulse(DxVisualQueue visual, Mark mark, int index, TimeSpan offset, byte alpha)
        {
            var price = mark.Price.Value;
            var x = GetX(index);
            if (mark.PriorMoveBps.HasValue)
            {
                var recent = mark.RecentMoveBps ?? 0;
                var startPrice = price / (1 + recent / 10000) / (1 + mark.PriorMoveBps.Value / 10000);
                var startIndex = BarIndex(ChartTime(mark.At - 45000, offset));
                var sx = startIndex >= 0 ? GetX(startIndex) : x;
                if (Math.Abs(sx - x) < 3)
                    sx = x - Canvas.ColumnWidth;  // the whole impulse inside one bar: a short slanted leg
                var impulseBrush = SideBrush(mark.PriorMoveBps.Value > 0 ? 1 : -1, (byte)(alpha * 0.8));
                visual.DrawLine(new XPen(impulseBrush, 2), sx, GetY(startPrice), x, GetY(price));
            }
            if (mark.Buys.HasValue && mark.Sells.HasValue)
            {
                var max = Math.Max(1, Math.Max(mark.Buys.Value, mark.Sells.Value));
                const double width = 46;
                var top = GetY(price) + 10;
                var left = x - width - 6;
                var buyWidth = width * mark.Buys.Value / max;
                var sellWidth = width * mark.Sells.Value / max;
                visual.FillRectangle(Buy((byte)(alpha * 0.9)), new Rect(left + width - buyWidth, top, Math.Max(2, buyWidth), 5));
                visual.FillRectangle(Sell((byte)(alpha * 0.9)), new Rect(left + width - sellWidth, top + 7, Math.Max(2, sellWidth), 5));
            }
        }

        // What happened next, from the chart's own bars: price at up to +5 minutes and the best/worst move band.
        private void DrawOutcome(DxVisualQueue visual, Mark mark, int index, TimeSpan offset, XFont font, byte alpha,
                                 List<Rect> taken)
        {
            var price = mark.Price.Value;
            var count = DataProvider.Count;
            var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var horizon = Math.Min(mark.At + 300000, nowMs);
            if (horizon - mark.At < 20000)
                return;  // too early to say anything
            var endIndex = BarIndex(ChartTime(horizon, offset));
            if (endIndex < index || endIndex >= count)
                return;
            double high = price, low = price;
            // The signal bar is skipped: most of its range happened before the card.
            for (var i = index + 1; i <= endIndex; i++)
            {
                var c = DataProvider.GetCluster(i);
                high = Math.Max(high, (double)c.High);
                low = Math.Min(low, (double)c.Low);
            }
            var close = (double)DataProvider.GetCluster(endIndex).Close;
            var x0 = GetX(index);
            var x1 = Math.Max(GetX(endIndex), x0 + Canvas.ColumnWidth);
            visual.FillRectangle(Neutral((byte)(alpha * 0.06)), new Rect(x0, GetY(high), x1 - x0, Math.Max(1, GetY(low) - GetY(high))));
            var move = close / price - 1;
            var good = mark.Side == 0 ? 0 : Math.Sign(move) == mark.Side ? 1 : -1;
            var brush = good > 0 ? Buy(alpha) : good < 0 ? Sell(alpha) : Neutral(alpha);
            visual.DrawLine(new XPen(brush, 1, XDashStyle.Dot), x0, GetY(price), x1, GetY(close));
            var minutes = Math.Max(1, (int)Math.Round((horizon - mark.At) / 60000.0));
            var label = $"{Pct(move)} за {minutes}м";
            if (mark.Side != 0)
            {
                // Magnitudes in the signal's favour and against it, whatever its side.
                var favour = Math.Max(0, mark.Side > 0 ? high / price - 1 : 1 - low / price);
                var against = Math.Max(0, mark.Side > 0 ? 1 - low / price : high / price - 1);
                label += $" · в пользу {favour * 100:0.00}% · против {against * 100:0.00}%";
            }
            else
            {
                label += $" (диапазон {Pct(low / price - 1)}…{Pct(high / price - 1)})";
            }
            // Fresh signals share the same end bar and price, so their captions would print over each other:
            // move each one down until it clears the captions already drawn.
            var height = _textSize * 1.3;
            var rect = new Rect(x1 + 4, GetY(close) - _textSize * 0.7, label.Length * _textSize * 0.6, height);
            while (taken.Exists(r => r.IntersectsWith(rect)))
                rect.Y += height;
            taken.Add(rect);
            visual.DrawString(label, font, brush, rect.X, rect.Y, XTextAlignment.Left);
        }

        // Zone box from when it appeared; thickness grows with its $ size. The latest zone runs to the edge.
        private void DrawZone(DxVisualQueue visual, Mark mark, TimeSpan offset, bool live, XFont font, byte alpha)
        {
            var y = GetY(mark.ZonePrice.Value);
            var rect = Canvas.Rect;
            if (y < rect.Top || y > rect.Bottom)
                return;
            var startIndex = mark.ZoneFirstSeen.HasValue ? BarIndex(ChartTime(mark.ZoneFirstSeen.Value, offset)) : -1;
            var endIndex = BarIndex(ChartTime(mark.At, offset));
            var x0 = startIndex >= 0 ? GetX(startIndex) : endIndex >= 0 ? GetX(endIndex) : rect.Left;
            var x1 = live ? rect.Right : endIndex >= 0 ? GetX(endIndex) + Canvas.ColumnWidth : x0 + Canvas.ColumnWidth;
            var usd = mark.ZoneUsd ?? 0;
            var thickness = Math.Max(3, Math.Min(14, 3 + Math.Log10(Math.Max(1, usd / 1000.0)) * 4));
            var side = mark.ZoneSide == "bid" ? 1 : -1;
            visual.FillRectangle(SideBrush(side, (byte)(alpha * 0.35)), new Rect(x0, y - thickness / 2, Math.Max(2, x1 - x0), thickness));
            visual.DrawLine(new XPen(SideBrush(side, alpha), 1, live ? XDashStyle.Dash : XDashStyle.Dot), x0, y, x1, y);
            if (!live)
                return;
            var label = (mark.ZoneSide == "bid" ? "Заявки на покупку" : "Заявки на продажу") + (usd > 0 ? " $" + Short(usd) : "");
            if (mark.ZoneEatSeconds.HasValue)
            {
                var eat = mark.ZoneEatSeconds.Value;
                label += " · съесть: " + (eat < 1 ? "<1с" : eat < 120 ? $"~{eat:0}с" : eat < 600 ? $"~{eat / 60:0} мин" : "10+ мин");
            }
            visual.DrawString(label, font, SideBrush(side, alpha), rect.Right - 6, y - thickness / 2 - _textSize * 1.5, XTextAlignment.Right);
        }

        private void DrawPanel(DxVisualQueue visual, Snapshot data, string error, XFont font, int drawn)
        {
            _panelRect = Rect.Empty;
            if (!_showPanel)
                return;
            var card = new List<string>();
            var panelMark = data?.Live ?? (data != null && data.Marks.Count > 0 ? data.Marks[data.Marks.Count - 1] : null);
            if (panelMark != null)
                foreach (var line in panelMark.Lines)
                    card.Add(Text(line));
            var notes = new List<string>();
            if (data != null && data.Marks.Count > 0 && drawn == 0)
                notes.Add($"сигналов за {_minutes} мин: {data.Marks.Count}, все вне видимой части графика");
            if (data != null && data.Live == null && panelMark != null)
                notes.Add("последний сигнал; инструмент сейчас вне внимания скринера");
            if (data != null && panelMark == null)
                notes.Add($"HyperData: сигналов по {_code} за {_minutes} мин нет");
            if (data == null && error == null)
                notes.Add("HyperData: загрузка…");
            if (error != null)
                notes.Add("HyperData: " + error + (data != null ? " — показаны прошлые данные" : ""));
            if (data?.Summary != null)
                notes.Insert(0, data.Summary);
            var full = new List<string>(card);
            full.AddRange(notes);
            _panelFull = full;
            // Collapsed: headline + verdict (+ notes). The whole card opens when the cursor is over the panel.
            var compact = new List<string>();
            if (card.Count > 0)
                compact.Add(card[0] + (card.Count > 2 ? "   ▸" : ""));
            if (card.Count > 1)
                compact.Add(card[1]);
            compact.AddRange(notes);
            if (compact.Count == 0)
                return;
            _panelRect = DrawBox(visual, compact, font, Canvas.Rect.Left + 8, Canvas.Rect.Top + _textSize * 2.2 + 8, panelMark?.Side ?? 0);
        }

        private Rect DrawBox(DxVisualQueue visual, List<string> lines, XFont font, double left, double top, int side)
        {
            var bold = new XFont(font.Name, font.Size, true);
            double width = 0, height = 0;
            foreach (var line in lines)
            {
                var measured = bold.GetSize(line);
                width = Math.Max(width, measured.Width);
                height += measured.Height + 2;
            }
            var rect = Canvas.Rect;
            left = Math.Max(rect.Left + 2, Math.Min(left, rect.Right - width - 20));
            top = Math.Max(rect.Top + 2, Math.Min(top, rect.Bottom - height - 14));
            var box = new Rect(left, top, width + 16, height + 10);
            visual.FillRectangle(Brush(240, 16, 20, 26), box);
            visual.DrawRectangle(new XPen(SideBrush(side, 160), 1), box);
            var y = box.Top + 5;
            for (var i = 0; i < lines.Count; i++)
            {
                var brush = i == 0 && side != 0 ? SideBrush(side, 255) : Brush(255, 230, 230, 230);
                visual.DrawString(lines[i], i == 0 ? bold : font, brush, box.Left + 8, y, XTextAlignment.Left);
                y += font.GetSize(lines[i]).Height + 2;
            }
            return box;
        }

        // Hover: the full card next to the cursor on a marker's bar; the full panel when over the panel.
        public override void RenderCursor(DxVisualQueue visual, int cursorPos, Point cursorCenter, ref int leftPos)
        {
            base.RenderCursor(visual, cursorPos, cursorCenter, ref leftPos);
            var font = new XFont(Canvas.ChartFont.Name, _textSize);
            if (!_panelRect.IsEmpty && _panelRect.Contains(cursorCenter) && _panelFull.Count > 0)
            {
                DrawBox(visual, _panelFull, font, _panelRect.Left, _panelRect.Top, 0);
                return;
            }
            var hits = new List<Placed>();
            foreach (var placed in _placed)
                if (placed.Index == cursorPos)
                    hits.Add(placed);
            if (hits.Count == 0)
                return;
            var mark = hits[hits.Count - 1].Mark;
            var lines = new List<string>();
            foreach (var line in mark.Lines)
                lines.Add(Text(line));
            var when = DateTimeOffset.FromUnixTimeMilliseconds(mark.At).ToLocalTime().ToString("HH:mm:ss");
            lines.Add(hits.Count > 1 ? $"{when} · ещё {hits.Count - 1} на этой свече" : when);
            DrawBox(visual, lines, font, cursorCenter.X + 16, cursorCenter.Y + 16, mark.Side);
        }

        // A Tiger alert (sound/popup per the indicator's alert settings) once per new card for this chart.
        private void RaiseAlert(Snapshot data)
        {
            long newest = 0;
            Mark latest = null;
            foreach (var mark in data.Marks)
                if (mark.At > newest) { newest = mark.At; latest = mark; }
            if (_alertedAt < 0)
            {
                _alertedAt = newest;  // history on first load is not news
                return;
            }
            if (latest == null || newest <= _alertedAt)
                return;
            _alertedAt = newest;
            if (Alert.IsActive)
                AddAlert(Alert, Text(string.Join(" · ", latest.Lines.GetRange(0, Math.Min(2, latest.Lines.Count)))));
        }

        public override void CopyTemplate(IndicatorBase indicator, bool style)
        {
            if (indicator is HyperDataSignalsIndicator other)
            {
                Url = other.Url;
                Minutes = other.Minutes;
                TextSize = other.TextSize;
                ShowPanel = other.ShowPanel;
                ShowOutcome = other.ShowOutcome;
                ShowImpulse = other.ShowImpulse;
                ShowEmoji = other.ShowEmoji;
                Alert.Copy(other.Alert, !style);
                OnPropertyChanged(nameof(Alert));
            }
            base.CopyTemplate(indicator, style);
        }
    }
}
