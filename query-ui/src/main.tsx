import React from 'react'
import { createRoot } from 'react-dom/client'
import { QueryEditor } from 'trino-query-ui'
import 'trino-query-ui/dist/index.css'
import './style.css'

createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryEditor />
  </React.StrictMode>,
)
