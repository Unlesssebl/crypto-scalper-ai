"""Model request transport for context reviews."""
import re

DIRECTIONAL = {"BULLISH", "BEARISH"}
KEY_FIELDS = ("symbol", "exchange", "contract", "window", "captured_at")
PROMPT = """Дай развёрнутую характеристику текущей ситуации Hyperliquid perpetual.
В summary напиши связный текст из нескольких предложений: как реагирует цена на поток,
что показывают интенсивность, волатильность и стакан, подтверждают ли картину другие окна.
Если метрики или окна расходятся, назови обе стороны расхождения и объясни,
что оно меняет в трактовке. Если сопоставление невозможно, укажи, каких данных не хватает.
Объединяй факты по смыслу. Не перечисляй все метрики подряд и не предлагай действий.
В scenario оставь одну короткую условную гипотезу. При слабой опоре на данные оставь пустую строку.
Не давай рекомендаций, торговых инструкций или целевой цены.
instrument_profile, если есть, описывает историческую норму инструмента и размер выборки.
Используй профиль как фон для сравнения, а не как доказательство текущего направления.
Используй как доказательства только метрики с quality=ok. Ссылайся на них по metric.
Направление требует подтверждения из нескольких независимых групп метрик.
CVD, OFI, число сделок и скорость ленты описывают один поток. Торговый объём даёт контекст.
NATR показывает типичный относительный диапазон, activity_ratio и range_ratio — всплески.
Всплески и волатильность показывают активность, направление ими не подтверждается.
first_trade_price и last_trade_price — наблюдавшиеся сделки на границах текущего окна,
используй их для контекста реакции цены. Для направления сопоставь поток с независимым
фактором, например с реакцией цены или стаканом.
OI, funding и стакан по отдельности не задают направление.
При нехватке данных выбери INSUFFICIENT_DATA. Раздели аргументы и контраргументы.
Не пересчитывай индикаторы, не добавляй числа или уровни, не давай команд на сделки.
Пиши по-русски. Характеристика — до 720 символов, гипотеза — до 200.
В каждом аргументе metric — ровно один точный ключ из metrics, без списков и объединений.
Формулируй сценарий как гипотезу.
Проверяй разные роли метрик: движение цены, направленный поток, интенсивность, волатильность и ликвидность.
Не считай несколько метрик одной роли отдельными подтверждениями.
context_1m описывает текущий импульс; context_5m даёт более широкий контекст.
flow_quality отдельно показывает доступность OFI и CVD. При статусе, отличном от ok,
не делай вывод о направлении или нейтральности потока этого окна.
movement_context содержит наблюдаемые возраст, расстояние и откат от максимума в NATR.
Положительное расстояние означает движение по наблюдаемому импульсу. Null означает неизвестное значение.
Учитывай, сколько движение уже прошло; отсутствие наблюдаемого начала не означает ранний импульс.
context_15m содержит фон по закрытым свечам: изменение за четыре свечи, их диапазон и положение последнего закрытия.
Если окна заметно расходятся, объясни это; старшее окно не отменяет автоматически краткий импульс.
Если есть btc_context, сравни последнюю закрытую свечу инструмента с BTC.
Корреляция доходностей описывает связь в прошедшем окне и сама не задаёт направление или причину движения.
market_context, если есть, — это рыночный фон: long/short-соотношение счетов (lsr),
spot-perp базис (basis), подразумеваемая волатильность (iv), ликвидации за час,
крупнейшие отслеживаемые позиции (whales) и экстремальный финансинг (extreme_funding).
Это контекст, а не доказательство направления текущего окна: используй его для оценки
риск-аппетита и уровня рынка, но не как аргумент направления в argument/contradiction.
В списках аргументов ссылайся только на ключи основного окна из metrics."""
def snapshot_key(snapshot):
    return {field: snapshot[field] for field in KEY_FIELDS}


class Gemini:
    model_pattern = r"[a-zA-Z0-9._-]+"

    def __init__(self, session, model, key, *, proxy=None):
        if not re.fullmatch(self.model_pattern, model):
            raise ValueError(f"Invalid model ID for {self.provider}")
        self.session, self.model, self.key = session, model, key
        self.proxy = proxy

    provider = "gemini"

    def request(self, market, schema):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        request = {
            "systemInstruction": {"parts": [{"text": PROMPT}]},
            "contents": [{"parts": [{"text": market}]}],
            "generationConfig": {
                "responseMimeType": "application/json", "responseJsonSchema": schema,
            },
        }
        return url, request, {"x-goog-api-key": self.key}

    def unpack(self, payload):
        candidate = payload["candidates"][0]
        if candidate.get("finishReason") != "STOP":
            return None, self.model
        raw = "".join(part.get("text", "") for part in candidate["content"]["parts"]
                      if not part.get("thought"))
        return raw, payload.get("modelVersion", self.model)



class OpenRouter(Gemini):
    provider = "openrouter"
    model_pattern = r"[a-zA-Z0-9._-]+/[a-zA-Z0-9._:-]+"

    def request(self, market, schema):
        request = {
            "model": self.model,
            "messages": [{"role": "system", "content": PROMPT}, {"role": "user", "content": market}],
            "provider": {"require_parameters": True},
            "max_tokens": 1000,
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "scalper_assessment", "strict": True, "schema": schema},
            },
        }
        if self.model.startswith("deepseek/deepseek-v4.1-flash"):
            request["reasoning"] = {"enabled": False}
        return "https://openrouter.ai/api/v1/chat/completions", request, {"Authorization": f"Bearer {self.key}"}

    def unpack(self, payload):
        choice = payload["choices"][0]
        if choice.get("finish_reason") != "stop":
            return None, self.model
        return choice["message"]["content"], payload.get("model", self.model)
