# Визуальная схема навигации к чанкам

## 🎯 Подход 1: Text Fragments API

```
┌─────────────────────────────────────────────────────────────┐
│  Приложение поиска                                          │
│                                                             │
│  User clicks result → Open URL:                            │
│  https://confluence.../page#:~:text=Медицинские            │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Браузер (Chrome/Edge/Safari)                               │
│                                                             │
│  1. Открывает страницу                                      │
│  2. Парсит #:~:text= fragment                               │
│  3. Ищет текст "Медицинские" на странице                    │
│  4. Скроллит к найденному тексту                            │
│  5. Подсвечивает фиолетовым цветом                          │
│                                                             │
│  ✅ Всё автоматически!                                      │
└─────────────────────────────────────────────────────────────┘
```

**Преимущества**: Нулевой код, работает из коробки

---

## 🎨 Подход 2: Custom JavaScript

```
┌─────────────────────────────────────────────────────────────┐
│  Приложение поиска                                          │
│                                                             │
│  User clicks → Extract navigation data:                     │
│  {                                                          │
│    xpath: "/html/body/div/p[5]",                           │
│    text: "Медицинские исследования..."                     │
│  }                                                          │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Страница Confluence (с внедренным JS)                      │
│                                                             │
│  1. Получить XPath из URL параметров                        │
│  2. const el = document.evaluate(xpath, ...)               │
│  3. el.scrollIntoView({ behavior: 'smooth' })              │
│  4. el.innerHTML = el.innerHTML.replace(text,              │
│        '<mark class="highlight">text</mark>')              │
│  5. Применить CSS стили для подсветки                       │
│                                                             │
│  ✅ Полный контроль над дизайном!                           │
└─────────────────────────────────────────────────────────────┘
```

**Преимущества**: Кастомные стили, работает везде

---

## 🔧 Подход 3: Browser Extension

```
┌─────────────────────────────────────────────────────────────┐
│  Приложение поиска                                          │
│                                                             │
│  User clicks → Open URL with params:                        │
│  https://confluence.../page?chunk_xpath=...&chunk_text=...  │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  Browser Extension (Content Script)                         │
│                                                             │
│  1. Перехватывает загрузку страницы                         │
│  2. Читает параметры из URL                                 │
│  3. После загрузки DOM:                                     │
│     - Находит элемент по XPath                              │
│     - Скроллит к нему                                       │
│     - Подсвечивает текст                                    │
│  4. Показывает UI кнопку "Снять подсветку"                  │
│                                                             │
│  ✅ Максимальная интеграция!                                │
└─────────────────────────────────────────────────────────────┘
```

**Преимущества**: Работает везде, лучший UX

---

## 📊 Сравнительная таблица

```
┌──────────────────┬─────────┬────────────┬──────────────┬─────────────┐
│     Подход       │ Код     │ Браузеры   │ Кастомизация │ Надежность  │
├──────────────────┼─────────┼────────────┼──────────────┼─────────────┤
│ Text Fragments   │ 1 line  │ Chr,Edg,Saf│     ⭐       │   ⭐⭐⭐⭐    │
│ Custom JS (XPath)│ ~50 ln  │ Все        │   ⭐⭐⭐⭐⭐   │   ⭐⭐⭐     │
│ Custom JS (CSS)  │ ~50 ln  │ Все        │   ⭐⭐⭐⭐⭐   │   ⭐⭐      │
│ Mark.js Library  │ ~30 ln  │ Все        │   ⭐⭐⭐⭐    │   ⭐⭐⭐⭐⭐  │
│ Extension        │ ~200 ln │ Все        │   ⭐⭐⭐⭐⭐   │   ⭐⭐⭐⭐⭐  │
└──────────────────┴─────────┴────────────┴──────────────┴─────────────┘
```

---

## 🎬 User Journey

### Сценарий: Пользователь ищет "побочные эффекты препарата"

```
1. Поиск
   ┌────────────────────────────┐
   │ [Поиск]                    │
   │ побочные эффекты препарата │
   │ [🔍 Найти]                 │
   └────────────────────────────┘
   
2. Результаты
   ┌────────────────────────────────────────────┐
   │ 📄 Клинические исследования                │
   │ > Результаты > Безопасность                │
   │                                            │
   │ "Побочные эффекты препарата были           │
   │  минимальны. Медицинские исследования..."  │
   │                                            │
   │ 📊 MED | 🕐 08.02.2026                     │
   └─────────────────┬──────────────────────────┘
                     │ Click!
                     ▼
3. Переход на страницу Confluence
   ┌────────────────────────────────────────────┐
   │ confluence.example.com/...                 │
   │ ──────────────────────────────────────     │
   │                                            │
   │ [Результаты]                               │
   │ [Эффективность]                            │
   │ [Безопасность]  ← Автоматически скроллит   │
   │                                            │
   │ Побочные эффекты препарата были            │
   │ минимальны. Медицинские исследования       │
   │ █████████████████████ ← Подсвечено!        │
   │                                            │
   └────────────────────────────────────────────┘
```

---

## 💻 Код для каждого подхода

### Минимальный (Text Fragments)

```javascript
// 1 строка!
window.open(chunk.navigation.url, '_blank');
```

### С fallback

```javascript
// ~10 строк
function openChunk(chunk) {
  if ('fragmentDirective' in document) {
    window.open(chunk.navigation.url, '_blank');
  } else {
    // Fallback для Firefox
    const url = `${chunk.navigation.url.split('#')[0]}?highlight=${encodeURIComponent(chunk.normalized_text)}`;
    window.open(url, '_blank');
  }
}
```

### Production-ready

```javascript
// ~50 строк с loading, error handling, analytics
async function openChunkWithTracking(chunk) {
  // Show loading
  showLoader('Открываем результат...');
  
  // Track analytics
  trackEvent('search_result_clicked', {
    chunk_id: chunk.chunk_id,
    page_id: chunk.page_id
  });
  
  try {
    // Try Text Fragments first
    if ('fragmentDirective' in document) {
      window.open(chunk.navigation.url, '_blank');
    } else {
      // Fallback
      await navigateWithCustomHighlight(chunk);
    }
  } catch (error) {
    console.error('Navigation failed:', error);
    showError('Не удалось открыть результат');
  } finally {
    hideLoader();
  }
}
```

---

## 🎨 Примеры визуального оформления

### Стандартная подсветка (Text Fragments)

```
Текст на странице с подсвеченной частью результата поиска.
             ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
            [фиолетовый фон от браузера]
```

### Желтая подсветка (Custom)

```
Текст на странице с подсвеченной частью результата поиска.
             ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
              [желтый фон #FFEB3B]
```

### Gradient подсветка (Custom)

```
Текст на странице с подсвеченной частью результата поиска.
             ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
        [gradient: голубой → зеленый]
```

### С обводкой (Custom)

```
Текст на странице с подсвеченной частью результата поиска.
             ┌─────────────────────────┐
             │ желтый фон + синяя рамка│
             └─────────────────────────┘
```

---

## 🚀 Quick Start Guide

### Шаг 1: Получите данные чанка

```javascript
const chunk = {
  navigation: {
    url: "https://confluence.../page#:~:text=Medical%20research",
    xpath_start: "/html/body/div/p[5]",
    css_selector_start: "article > p.content",
    highlight_metadata: {
      text_fragment: "Medical research shows..."
    }
  },
  normalized_text: "Medical research shows effectiveness...",
  page_title: "Clinical Studies"
};
```

### Шаг 2: Откройте результат

```javascript
// Самый простой способ:
<a href={chunk.navigation.url} target="_blank">
  Open result
</a>

// Или с JavaScript:
onClick={() => window.open(chunk.navigation.url, '_blank')}
```

### Шаг 3: Готово! ✨

Браузер автоматически подсветит текст.

---

## 📚 Дополнительные ресурсы

- **Text Fragments Spec**: https://wicg.github.io/scroll-to-text-fragment/
- **Mark.js Library**: https://markjs.io/
- **XPath Tutorial**: https://www.w3schools.com/xml/xpath_intro.asp
- **Browser Extension Guide**: https://developer.chrome.com/docs/extensions/

---

## ✅ Checklist для реализации

- [ ] Используйте `navigation.url` как основной метод
- [ ] Добавьте loading indicator
- [ ] Откройте в новой вкладке (`_blank`)
- [ ] Отслеживайте клики для analytics
- [ ] Добавьте fallback для старых браузеров
- [ ] Тестируйте в разных браузерах
- [ ] Добавьте error handling
- [ ] Документируйте для команды

---

**Готово!** Теперь у вас есть всё для реализации навигации к чанкам! 🎉
