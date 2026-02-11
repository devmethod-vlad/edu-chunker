# Шпаргалка: Навигация к чанкам

## 🚀 Быстрый старт (1 строка кода)

```javascript
window.open(chunk.navigation.url, '_blank');
```

**Готово!** Браузер автоматически:
- ✅ Откроет страницу
- ✅ Найдет текст
- ✅ Проскроллит к нему
- ✅ Подсветит его

---

## 📋 Доступные данные в chunk.navigation

```javascript
{
  // Готовый URL с Text Fragment - просто откройте его!
  url: "https://confluence.../page#:~:text=Медицинские%20исследования",
  
  // XPath до первого элемента чанка
  xpath_start: "/html/body/div[2]/main/article/p[5]",
  
  // CSS селектор первого элемента
  css_selector_start: "div.content > article > p.paragraph",
  
  // Смещение текста от начала страницы (в символах)
  text_offset_start: 1523,
  
  // Длина normalized_text (в символах)
  text_length: 247,
  
  // Дополнительные метаданные
  highlight_metadata: {
    text_fragment: "Медицинские исследования показывают...",  // Первые 100 символов
    block_type: "p",
    text_offset: 1523
  }
}
```

---

## 🎯 Выбор подхода

### Вариант 1: Только Text Fragments (90% случаев) ⭐

```javascript
// Самое простое решение
<a href={chunk.navigation.url} target="_blank">
  Открыть результат
</a>
```

**Когда использовать**: Всегда, если не нужен кастомный дизайн подсветки

**Поддержка**: Chrome ✅ | Edge ✅ | Safari 16.1+ ✅ | Firefox ❌ (скоро)

---

### Вариант 2: Custom JS с XPath (полный контроль)

```javascript
function navigateToChunk(chunk) {
  // 1. Найти элемент
  const xpath = chunk.navigation.xpath_start;
  const element = document.evaluate(
    xpath, document, null, 
    XPathResult.FIRST_ORDERED_NODE_TYPE, null
  ).singleNodeValue;
  
  // 2. Скроллить
  element?.scrollIntoView({ behavior: 'smooth', block: 'center' });
  
  // 3. Подсветить
  element.innerHTML = element.innerHTML.replace(
    chunk.normalized_text.substring(0, 100),
    `<mark class="highlight">$&</mark>`
  );
}
```

**Когда использовать**: Нужен кастомный цвет/стиль подсветки

**Поддержка**: Все браузеры ✅

---

### Вариант 3: Browser Extension (максимальная интеграция)

**Для enterprise решений** где пользователи устанавливают extension

1. Extension слушает URL параметры
2. Автоматически подсвечивает при открытии страницы
3. Работает на всех страницах Confluence

---

## 🎨 Примеры кастомной подсветки

### Желтая подсветка

```css
mark.search-highlight {
  background-color: #FFEB3B;
  padding: 2px 4px;
  border-radius: 3px;
}
```

### Подсветка с анимацией

```css
mark.search-highlight {
  background-color: #FFEB3B;
  animation: pulse 1s ease-in-out;
}

@keyframes pulse {
  0%, 100% { background-color: #FFEB3B; }
  50% { background-color: #FFC107; }
}
```

### Gradient подсветка

```css
mark.search-highlight {
  background: linear-gradient(120deg, #84fab0 0%, #8fd3f4 100%);
  padding: 2px 6px;
  border-radius: 4px;
  font-weight: 500;
}
```

---

## 🔧 Troubleshooting

### Проблема: Text Fragment не работает

**Причина**: Браузер не поддерживает или текст не найден

**Решение**: Используйте fallback

```javascript
if ('fragmentDirective' in document) {
  window.open(chunk.navigation.url);
} else {
  // Fallback на XPath/CSS
  navigateWithXPath(chunk);
}
```

---

### Проблема: XPath не находит элемент

**Причина**: Структура HTML изменилась

**Решение**: Используйте text search как запасной вариант

```javascript
const element = getElementByXPath(xpath) 
  || document.querySelector(cssSelector)
  || findElementByText(chunk.navigation.highlight_metadata.text_fragment);
```

---

### Проблема: Confluence не позволяет внедрить JS

**Решение**: Browser extension или используйте только Text Fragments

---

## 💡 Best Practices

1. ✅ **Всегда используйте `navigation.url` как основной метод**
2. ✅ **Добавьте fallback для старых браузеров**
3. ✅ **Показывайте loading indicator при переходе**
4. ✅ **Открывайте в новой вкладке** (`target="_blank"`)
5. ✅ **Добавьте кнопку "Снять подсветку"** для UX

---

## 📱 React компонент (готовый к использованию)

```jsx
import React from 'react';

function SearchResultItem({ chunk }) {
  const handleClick = () => {
    // Просто открываем URL - браузер сделает всё сам!
    window.open(chunk.navigation.url, '_blank');
  };
  
  return (
    <div 
      className="search-result"
      onClick={handleClick}
      style={{ cursor: 'pointer' }}
    >
      <h3>{chunk.page_title}</h3>
      
      {/* Breadcrumb */}
      <div className="breadcrumb">
        {chunk.text_heading_hierarchy.join(' > ')}
      </div>
      
      {/* Snippet */}
      <p className="snippet">
        {chunk.normalized_text.substring(0, 200)}...
      </p>
      
      {/* Metadata */}
      <div className="metadata">
        <span>📄 {chunk.space_key}</span>
        <span>🕐 {new Date(chunk.last_modified).toLocaleDateString()}</span>
      </div>
    </div>
  );
}

export default SearchResultItem;
```

---

## 🎯 Итого

Для 90% случаев достаточно:

```javascript
<a href={chunk.navigation.url} target="_blank">
  Результат поиска
</a>
```

Браузер сделает всё остальное! 🎉

Подробности в **NAVIGATION_GUIDE.md**
