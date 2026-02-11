# Руководство по навигации и подсветке чанков

## Обзор

После поиска пользователь получает список чанков. При клике на результат нужно:
1. **Перейти** к нужному месту на странице
2. **Подсветить** найденный текст
3. **Проскроллить** страницу к чанку

У нас есть несколько подходов, от простых к сложным.

---

## 🎯 Подход 1: Text Fragments API (Рекомендуется)

**Самый простой и надежный способ!**

### Что это?

Text Fragments API - стандарт браузеров, позволяющий создавать ссылки с **автоматической подсветкой** текста.

### Как использовать

```javascript
// У нас уже есть готовый URL в navigation.url
const navigationUrl = chunk.navigation.url;
// Пример: "https://confluence.../page#:~:text=Медицинские%20исследования"

// Просто открываем URL
window.location.href = navigationUrl;
// ИЛИ
window.open(navigationUrl, '_blank');
```

### Что происходит

1. ✅ Браузер **автоматически** находит текст на странице
2. ✅ **Скроллит** к нему
3. ✅ **Подсвечивает** текст фиолетовым цветом (стандартная подсветка браузера)

### Преимущества

- ✅ Нулевой код на стороне клиента
- ✅ Работает из коробки
- ✅ Поддерживается Chrome, Edge, Safari (с версии 16.1)
- ✅ Не требует доступа к DOM

### Ограничения

- ⚠️ Firefox пока не поддерживает (в разработке)
- ⚠️ Цвет подсветки нельзя кастомизировать (фиолетовый по умолчанию)
- ⚠️ Подсветка исчезает через несколько секунд

### Пример реализации в приложении поиска

```javascript
// React компонент результатов поиска
function SearchResult({ chunk }) {
  const handleClick = () => {
    // Просто открываем URL с text fragment
    window.open(chunk.navigation.url, '_blank');
  };
  
  return (
    <div className="search-result" onClick={handleClick}>
      <h3>{chunk.page_title}</h3>
      <div className="breadcrumb">
        {chunk.text_heading_hierarchy.join(' > ')}
      </div>
      <p className="snippet">{chunk.normalized_text.substring(0, 200)}...</p>
    </div>
  );
}
```

**Вывод**: Для 90% случаев этого достаточно! ✨

---

## 🎨 Подход 2: Custom JavaScript Highlighting

**Когда нужен полный контроль над подсветкой.**

### Когда использовать

- Нужен кастомный цвет/стиль подсветки
- Нужна постоянная подсветка (не исчезающая)
- Поддержка Firefox обязательна
- Confluence настроен так, что можно внедрить custom JS

### Вариант 2.1: С использованием XPath

```javascript
/**
 * Переход к чанку и подсветка с использованием XPath
 */
function navigateToChunk(chunk) {
  const { xpath_start, text_length } = chunk.navigation;
  const searchText = chunk.normalized_text;
  
  // 1. Находим элемент по XPath
  const element = getElementByXPath(xpath_start);
  
  if (!element) {
    console.error('Element not found by XPath:', xpath_start);
    return;
  }
  
  // 2. Скроллим к элементу
  element.scrollIntoView({ 
    behavior: 'smooth', 
    block: 'center' 
  });
  
  // 3. Подсвечиваем текст
  highlightTextInElement(element, searchText);
}

/**
 * Получение элемента по XPath
 */
function getElementByXPath(xpath) {
  const result = document.evaluate(
    xpath,
    document,
    null,
    XPathResult.FIRST_ORDERED_NODE_TYPE,
    null
  );
  return result.singleNodeValue;
}

/**
 * Подсветка текста в элементе
 */
function highlightTextInElement(element, searchText) {
  // Получаем текстовое содержимое
  const text = element.textContent;
  
  // Ищем текст (первые 100 символов для надежности)
  const searchFragment = searchText.substring(0, 100);
  const index = text.indexOf(searchFragment);
  
  if (index === -1) {
    console.warn('Text not found in element');
    return;
  }
  
  // Оборачиваем текст в span с подсветкой
  const innerHTML = element.innerHTML;
  const textToHighlight = text.substring(index, index + searchText.length);
  
  element.innerHTML = innerHTML.replace(
    textToHighlight,
    `<mark class="search-highlight">${textToHighlight}</mark>`
  );
  
  // Добавляем CSS для подсветки
  addHighlightStyles();
}

/**
 * Добавление стилей подсветки
 */
function addHighlightStyles() {
  if (document.getElementById('search-highlight-styles')) return;
  
  const style = document.createElement('style');
  style.id = 'search-highlight-styles';
  style.textContent = `
    .search-highlight {
      background-color: #ffeb3b;
      padding: 2px 0;
      border-radius: 2px;
      animation: highlight-pulse 1s ease-in-out;
    }
    
    @keyframes highlight-pulse {
      0%, 100% { background-color: #ffeb3b; }
      50% { background-color: #ffc107; }
    }
  `;
  document.head.appendChild(style);
}
```

### Вариант 2.2: С использованием CSS селектора

```javascript
/**
 * Переход к чанку используя CSS селектор
 */
function navigateToChunkBySelector(chunk) {
  const { css_selector_start } = chunk.navigation;
  const searchText = chunk.normalized_text;
  
  // 1. Находим элемент по CSS селектору
  const element = document.querySelector(css_selector_start);
  
  if (!element) {
    console.error('Element not found by selector:', css_selector_start);
    return;
  }
  
  // 2. Скроллим к элементу
  element.scrollIntoView({ 
    behavior: 'smooth', 
    block: 'center' 
  });
  
  // 3. Подсвечиваем
  highlightTextInElement(element, searchText);
}
```

### Вариант 2.3: Продвинутая подсветка с Mark.js

Используем библиотеку [Mark.js](https://markjs.io/) для надежной подсветки:

```javascript
/**
 * Подсветка с использованием Mark.js
 */
import Mark from 'mark.js';

function highlightChunkWithMarkJS(chunk) {
  const { css_selector_start } = chunk.navigation;
  const searchText = chunk.normalized_text;
  
  // Находим элемент
  const element = document.querySelector(css_selector_start);
  
  if (!element) return;
  
  // Скроллим
  element.scrollIntoView({ 
    behavior: 'smooth', 
    block: 'center' 
  });
  
  // Используем Mark.js для подсветки
  const markInstance = new Mark(element);
  
  // Подсвечиваем первые 50 слов чанка
  const wordsToHighlight = searchText.split(' ').slice(0, 50).join(' ');
  
  markInstance.mark(wordsToHighlight, {
    accuracy: 'partially',
    className: 'search-highlight',
    caseSensitive: false,
    separateWordSearch: false,
    done: function() {
      // Скроллим к первому найденному совпадению
      const firstMark = element.querySelector('.search-highlight');
      if (firstMark) {
        firstMark.scrollIntoView({ 
          behavior: 'smooth', 
          block: 'center' 
        });
      }
    }
  });
}
```

---

## 🔧 Подход 3: Комбинированный (Наиболее надежный)

**Использует несколько методов с fallback.**

```javascript
/**
 * Умная навигация с fallback стратегиями
 */
async function navigateToChunkSmart(chunk) {
  const { url, xpath_start, css_selector_start, highlight_metadata } = chunk.navigation;
  
  // Стратегия 1: Text Fragments API (если поддерживается)
  if (isTextFragmentsSupported()) {
    window.location.href = url;
    return;
  }
  
  // Стратегия 2: XPath
  let element = getElementByXPath(xpath_start);
  
  // Стратегия 3: CSS селектор (fallback)
  if (!element) {
    element = document.querySelector(css_selector_start);
  }
  
  // Стратегия 4: Поиск по тексту (последний fallback)
  if (!element) {
    element = findElementByText(highlight_metadata.text_fragment);
  }
  
  if (!element) {
    console.error('Could not locate chunk on page');
    // Просто открываем страницу
    window.location.href = chunk.navigation.url.split('#')[0];
    return;
  }
  
  // Нашли элемент - позиционируем и подсвечиваем
  element.scrollIntoView({ behavior: 'smooth', block: 'center' });
  
  // Добавляем визуальный индикатор
  highlightElement(element, chunk.normalized_text);
}

/**
 * Проверка поддержки Text Fragments API
 */
function isTextFragmentsSupported() {
  return 'fragmentDirective' in document;
}

/**
 * Поиск элемента по фрагменту текста
 */
function findElementByText(textFragment) {
  const walker = document.createTreeWalker(
    document.body,
    NodeFilter.SHOW_TEXT,
    null
  );
  
  let node;
  while (node = walker.nextNode()) {
    if (node.textContent.includes(textFragment)) {
      return node.parentElement;
    }
  }
  
  return null;
}

/**
 * Подсветка элемента с анимацией
 */
function highlightElement(element, text) {
  // Временно подсвечиваем весь элемент
  element.classList.add('chunk-highlight-temp');
  
  // Через секунду подсвечиваем только текст
  setTimeout(() => {
    element.classList.remove('chunk-highlight-temp');
    highlightTextInElement(element, text);
  }, 1000);
}
```

**CSS для подсветки:**

```css
/* Временная подсветка всего элемента */
.chunk-highlight-temp {
  outline: 3px solid #2196F3;
  outline-offset: 2px;
  background-color: rgba(33, 150, 243, 0.1);
  animation: pulse-border 1s ease-in-out;
}

@keyframes pulse-border {
  0%, 100% { outline-color: #2196F3; }
  50% { outline-color: #64B5F6; }
}

/* Подсветка текста */
mark.search-highlight {
  background-color: #FFEB3B;
  color: #000;
  padding: 2px 4px;
  border-radius: 3px;
  font-weight: 500;
  box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  animation: highlight-fade-in 0.5s ease-in;
}

@keyframes highlight-fade-in {
  from {
    background-color: #FFC107;
    transform: scale(1.05);
  }
  to {
    background-color: #FFEB3B;
    transform: scale(1);
  }
}
```

---

## 🎪 Подход 4: Browser Extension / User Script

**Для максимальной интеграции с Confluence.**

Создаем browser extension (Chrome/Firefox), которое:

1. Слушает параметры URL с информацией о чанке
2. Автоматически находит и подсвечивает текст
3. Работает на всех страницах Confluence

### Структура Extension

```
confluence-highlighter/
├── manifest.json
├── content.js       # Скрипт, выполняющийся на страницах Confluence
└── background.js    # Background service worker
```

### manifest.json

```json
{
  "manifest_version": 3,
  "name": "Confluence Chunk Highlighter",
  "version": "1.0",
  "description": "Highlights search results in Confluence pages",
  
  "content_scripts": [
    {
      "matches": ["*://your-confluence-domain.com/*"],
      "js": ["content.js"],
      "run_at": "document_end"
    }
  ],
  
  "permissions": ["activeTab"]
}
```

### content.js

```javascript
/**
 * Content script для подсветки чанков на странице Confluence
 */
(function() {
  'use strict';
  
  // Проверяем наличие параметров навигации в URL
  const params = new URLSearchParams(window.location.search);
  
  // Параметры можно передать так:
  // ?chunk_xpath=/html/body/div/p&chunk_text=Медицинские%20исследования
  const chunkXPath = params.get('chunk_xpath');
  const chunkSelector = params.get('chunk_selector');
  const chunkText = params.get('chunk_text');
  
  if (!chunkXPath && !chunkSelector && !chunkText) {
    // Нет информации о чанке - выходим
    return;
  }
  
  // Ждем полной загрузки страницы
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', highlightChunk);
  } else {
    highlightChunk();
  }
  
  function highlightChunk() {
    let element = null;
    
    // Пробуем найти элемент
    if (chunkXPath) {
      element = getElementByXPath(chunkXPath);
    }
    
    if (!element && chunkSelector) {
      element = document.querySelector(chunkSelector);
    }
    
    if (!element && chunkText) {
      element = findElementByText(chunkText);
    }
    
    if (!element) {
      console.warn('Could not find chunk element');
      return;
    }
    
    // Подсвечиваем и скроллим
    scrollToAndHighlight(element, chunkText);
  }
  
  function scrollToAndHighlight(element, text) {
    // Скролл с отступом для навигации
    const yOffset = -100;
    const y = element.getBoundingClientRect().top + window.pageYOffset + yOffset;
    
    window.scrollTo({ top: y, behavior: 'smooth' });
    
    // Подсвечиваем
    if (text) {
      highlightTextInElement(element, text);
    } else {
      // Подсвечиваем весь элемент
      element.style.backgroundColor = '#FFEB3B';
      element.style.transition = 'background-color 2s ease-out';
      
      setTimeout(() => {
        element.style.backgroundColor = '';
      }, 2000);
    }
  }
  
  // Вспомогательные функции...
  function getElementByXPath(xpath) { /* ... */ }
  function findElementByText(text) { /* ... */ }
  function highlightTextInElement(element, text) { /* ... */ }
})();
```

### Использование из приложения поиска

```javascript
// При клике на результат поиска
function openChunkInConfluence(chunk) {
  const baseUrl = chunk.navigation.url.split('#')[0];
  
  // Формируем URL с параметрами для extension
  const url = new URL(baseUrl);
  url.searchParams.set('chunk_xpath', chunk.navigation.xpath_start);
  url.searchParams.set('chunk_selector', chunk.navigation.css_selector_start);
  url.searchParams.set('chunk_text', chunk.navigation.highlight_metadata.text_fragment);
  
  // Открываем в новой вкладке
  window.open(url.toString(), '_blank');
}
```

---

## 📊 Сравнение подходов

| Подход | Сложность | Надежность | Кастомизация | Browser Support |
|--------|-----------|------------|--------------|-----------------|
| Text Fragments | ⭐ Очень просто | ⭐⭐⭐⭐ | ⭐ Низкая | Chrome, Edge, Safari 16.1+ |
| Custom JS (XPath) | ⭐⭐ Средне | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Все браузеры |
| Custom JS (CSS) | ⭐⭐ Средне | ⭐⭐ | ⭐⭐⭐⭐⭐ | Все браузеры |
| Mark.js | ⭐⭐⭐ Средне | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Все браузеры |
| Комбинированный | ⭐⭐⭐⭐ Сложно | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Все браузеры |
| Browser Extension | ⭐⭐⭐⭐⭐ Очень сложно | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Требует установки |

---

## 🎯 Рекомендации

### Для быстрого MVP:
✅ **Используйте Text Fragments API** (подход 1)
- Нулевой код
- Работает из коробки
- Достаточно для 90% случаев

### Для production системы:
✅ **Комбинированный подход** (подход 3)
- Text Fragments как основной метод
- Custom highlighting как fallback
- Поддержка всех браузеров

### Для максимальной интеграции:
✅ **Browser Extension** (подход 4)
- Полный контроль
- Лучший UX
- Требует установки пользователями

---

## 💡 Дополнительные улучшения

### 1. Индикатор загрузки

```javascript
function navigateWithLoader(chunk) {
  // Показываем индикатор
  showLoadingIndicator('Переход к результату...');
  
  // Открываем страницу
  const newWindow = window.open(chunk.navigation.url, '_blank');
  
  // Проверяем загрузку (если в том же домене)
  if (newWindow) {
    const checkLoaded = setInterval(() => {
      try {
        if (newWindow.document.readyState === 'complete') {
          clearInterval(checkLoaded);
          hideLoadingIndicator();
        }
      } catch (e) {
        // Cross-origin - не можем проверить
        clearInterval(checkLoaded);
        hideLoadingIndicator();
      }
    }, 100);
  }
}
```

### 2. Кнопка "Снять подсветку"

```javascript
// Добавляем плавающую кнопку на странице Confluence
function addClearHighlightButton() {
  const button = document.createElement('button');
  button.id = 'clear-highlight-btn';
  button.textContent = '✕ Снять подсветку';
  button.style.cssText = `
    position: fixed;
    top: 80px;
    right: 20px;
    z-index: 9999;
    padding: 10px 20px;
    background: #f44336;
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    box-shadow: 0 2px 10px rgba(0,0,0,0.2);
  `;
  
  button.onclick = () => {
    // Удаляем все подсветки
    document.querySelectorAll('.search-highlight').forEach(el => {
      el.outerHTML = el.textContent;
    });
    button.remove();
  };
  
  document.body.appendChild(button);
}
```

### 3. Контекстное превью

```javascript
// Показываем превью чанка перед переходом
function showChunkPreview(chunk) {
  const preview = document.createElement('div');
  preview.className = 'chunk-preview';
  preview.innerHTML = `
    <div class="preview-header">
      <span>${chunk.page_title}</span>
      <button onclick="this.parentElement.parentElement.remove()">✕</button>
    </div>
    <div class="preview-breadcrumb">
      ${chunk.text_heading_hierarchy.join(' > ')}
    </div>
    <div class="preview-content">
      ${chunk.normalized_text}
    </div>
    <div class="preview-actions">
      <button onclick="window.open('${chunk.navigation.url}', '_blank')">
        Открыть на странице
      </button>
    </div>
  `;
  
  document.body.appendChild(preview);
}
```

---

## 🔍 Отладка

### Проверка Text Fragments в консоли браузера

```javascript
// Проверка поддержки
console.log('Text Fragments supported:', 'fragmentDirective' in document);

// Тест подсветки
window.location.hash = ':~:text=какой-то текст со страницы';
```

### Проверка XPath

```javascript
// Тестирование XPath в консоли
const xpath = '/html/body/div[2]/main/article/p[3]';
const result = document.evaluate(xpath, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
console.log('Element found:', result.singleNodeValue);
```

### Проверка CSS селектора

```javascript
const selector = 'div.content > article > p:nth-child(3)';
const element = document.querySelector(selector);
console.log('Element found:', element);
```

---

## 📝 Итоговая рекомендация

**Начните с Text Fragments API**, это самый простой способ:

```javascript
// Минимальная реализация
function openSearchResult(chunk) {
  window.open(chunk.navigation.url, '_blank');
}
```

**Затем добавьте fallback** для полной поддержки:

```javascript
// С fallback
function openSearchResult(chunk) {
  if ('fragmentDirective' in document) {
    // Современные браузеры - Text Fragments
    window.open(chunk.navigation.url, '_blank');
  } else {
    // Старые браузеры - custom highlighting
    const url = chunk.navigation.url.split('#')[0];
    window.open(
      `${url}?highlight_xpath=${encodeURIComponent(chunk.navigation.xpath_start)}`,
      '_blank'
    );
  }
}
```

Этого хватит для отличного пользовательского опыта! 🎉
